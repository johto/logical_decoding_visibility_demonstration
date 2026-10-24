// This program demonstrates how a row received from a logical decoding stream
// is not necessarily visible on the source server.  We set up a pg_receivewal
// instance to act as a synchronous physical replica, then one goroutine
// INSERTs rows in a loop while another reads them from "pglogical" output and
// tries to UPDATE the row immediately.  This consistently fails because
// postgres waits for the synchronous replica to respond before marking the row
// locally visible.  (The window is there even without synchronous replica, but
// smaller in practice.
//
// In order to test, run:
//
//   docker run --rm -v "$PWD":/src -w /src -e HOME=/src --user "$(id -u):$(id -g)" golang:1.25-bookworm bash -c 'go mod init logvis && go mod tidy && go build'
//   docker run --name logvis -v "$PWD:/mnt/run" -e 'POSTGRES_PASSWORD=null' --rm -it postgres:18-bookworm -c 'wal_level=logical' -c 'synchronous_standby_names=syncrep'
//
// and then in another session:
//
//   docker exec -it logvis bash -c 'PGUSER=postgres PGDATABASE=postgres /mnt/run/logvis'
//
// If you omit synchronous_standby_names from the second command, the problem
// is less likely to happen.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type txnProcessor struct {
	ordersTableOid uint32

	inTxn bool
	orderIDs []int
	orderCh chan<- int
}

func (proc *txnProcessor) handlePgOutputMessage(walData []byte) {
	if walData[0] == 'B' {
		if proc.inTxn {
			panic("inTxn")
		}
		proc.inTxn = true
	} else if walData[0] == 'R' {
		// we already know the oid
	} else if walData[0] == 'I' {
		if binary.BigEndian.Uint32(walData[1:5]) != proc.ordersTableOid {
			panic(fmt.Sprintf("%q", walData))
		}
		if walData[5] != 'N' {
			panic(fmt.Sprintf("%q", walData))
		}
		numCols := binary.BigEndian.Uint16(walData[6:8])
		if numCols != 2 {
			panic(numCols)
		}
		orderID := int(binary.BigEndian.Uint32(walData[13:17]))
		proc.orderIDs = append(proc.orderIDs, orderID)
	} else if walData[0] == 'C' {
		if !proc.inTxn {
			panic("!inTxn")
		}

		for _, id := range proc.orderIDs {
			proc.orderCh <- id
		}

		proc.inTxn = false
		proc.orderIDs = nil
	} else if walData[0] == 'U' {
		// don't care
	} else {
		panic(fmt.Sprintf("%q", walData))
	}
}

func parseOrderID(walData []byte) int {
	fmt.Printf("%q\n", walData)
	return -1
}

func sendKeepalive(ctx context.Context, replConn *pgconn.PgConn, lsn pglogrepl.LSN) {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	standbyStatus := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		ClientTime:       time.Now(),
		ReplyRequested:   false,

		// not applicable
		WALApplyPosition: pglogrepl.LSN(0),
	}

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, standbyStatus); err != nil {
		panic(err)
	}
}

// XXX This function is obviously not good enough for a production client, so
// don't copy any parts of it.
func handleReplicationStream(parent context.Context, replConn *pgconn.PgConn, ordersTableOid uint32, orderCh chan<- int) {
	processor := &txnProcessor{
		ordersTableOid: ordersTableOid,

		inTxn: false,
		orderIDs: nil,
		orderCh: orderCh,
	}

	errTimeout := errors.New("timeout")
	lastLSN := pglogrepl.LSN(0)
	for {
		ctx, cancel := context.WithTimeoutCause(parent, 5 * time.Second, errTimeout)
		rawMessage, err := replConn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if !pgconn.Timeout(err) {
				panic(err)
			}
			sendKeepalive(parent, replConn, lastLSN)
			continue
		}
		if errorResponse, ok := rawMessage.(*pgproto3.ErrorResponse); ok {
			panic(fmt.Errorf("%s:  %s", errorResponse.Severity, errorResponse.Message))
		} else if noticeResponse, ok := rawMessage.(*pgproto3.NoticeResponse); ok {
			panic(fmt.Errorf("%s:  %s\n", noticeResponse.Severity, noticeResponse.Message))
		}
		msg, ok := rawMessage.(*pgproto3.CopyData)
		if !ok {
			panic(fmt.Errorf("received unexpected message: %T", rawMessage))
		}
		if msg.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				panic(fmt.Errorf("could not parse keepalive message: %w", err))
			}
			if keepalive.ReplyRequested {
				sendKeepalive(parent, replConn, lastLSN)
			}
		} else if msg.Data[0] == pglogrepl.XLogDataByteID {
			xlogData, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				panic(fmt.Errorf("could not parse XLogData message: %w", err))
			}
			processor.handlePgOutputMessage(xlogData.WALData)
			lastLSN = xlogData.WALStart + pglogrepl.LSN(len(xlogData.WALData)) + 1
		} else {
			panic(fmt.Errorf("unexpected replication stream message: 0x%02X", msg.Data[0]))
		}
	}
}

func startLogicalReplication(ctx context.Context, dbh *pgx.Conn) <-chan int {
	replConn, err := pgconn.Connect(ctx, "replication=database")
	if err != nil {
		panic(err)
	}

	system, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		panic(err)
	}

	err = pglogrepl.StartReplication(ctx, replConn, "visibility", system.XLogPos, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '4'",
			"publication_names 'visibility'",
			"binary 'true'",
			"streaming 'false'",
		},
	})
	if err != nil {
		panic(err)
	}

	var ordersTableOid uint32
	if err := dbh.QueryRow(ctx, `SELECT 'orders'::regclass::oid`).Scan(&ordersTableOid); err != nil {
		panic(err)
	}

	// we see the marker in the stream, we know everything is working.
	var markerOrderID int
	if err := dbh.QueryRow(ctx, `INSERT INTO orders DEFAULT VALUES RETURNING order_id`).Scan(&markerOrderID); err != nil {
		panic(err)
	}

	orderCh := make(chan int)
	go handleReplicationStream(ctx, replConn, ordersTableOid, orderCh)

	fmt.Println("waiting for marker order..")
	for {
		orderID := <-orderCh
		if orderID == markerOrderID {
			fmt.Println("yup")
			break
		}
	}
	return orderCh
}

func startPhysicalReplication(ctx context.Context) {
	cmd := exec.CommandContext(
		ctx,
		"pg_receivewal",
		"-D", "/tmp",
		"--synchronous",
		"--no-loop",
		"-d", "application_name=syncrep",
	)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr
	go func() {
		if err := cmd.Run(); err != nil {
			panic(err)
		}
	}()
}

func orderUpdater(ctx context.Context, orderCh <-chan int) {
	dbh, err := pgx.Connect(ctx, "")
	if err != nil {
		panic(err)
	}

	for {
		orderID := <-orderCh
		for {
			tag, err := dbh.Exec(ctx, `UPDATE orders SET entry_step_id = 1 WHERE order_id = $1`, orderID)
			if err != nil {
				panic(err)
			}
			if tag.RowsAffected() == 0 {
				fmt.Printf("order_id %d NOT VISIBLE at %s\n", orderID, time.Now().Format(time.RFC3339Nano))
				time.Sleep(time.Millisecond)
				continue
			} else if tag.RowsAffected() != 1 {
				panic(tag.RowsAffected())
			}

			fmt.Printf("order_id %d visible at %s\n", orderID, time.Now().Format(time.RFC3339Nano))
			break
		}
	}
}

func main() {
	ctx := context.Background()
	dbh, err := pgx.Connect(ctx, "")
	if err != nil {
		panic(err)
	}

	startPhysicalReplication(ctx)

	sql := `
SELECT pg_create_logical_replication_slot('visibility', 'pgoutput');

CREATE TABLE orders(
	order_id serial PRIMARY KEY,
	entry_step_id integer
);

CREATE PUBLICATION visibility FOR TABLE orders;
	`
	if _, err := dbh.Exec(ctx, sql); err != nil {
		panic(err)
	}

	orderCh := startLogicalReplication(ctx, dbh)
	go orderUpdater(ctx, orderCh)

	for {
		if _, err := dbh.Exec(ctx, `INSERT INTO orders DEFAULT VALUES`); err != nil {
			panic(err)
		}

		time.Sleep(50 * time.Millisecond)
	}
}
