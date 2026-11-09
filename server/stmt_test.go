package server

import (
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/stmt"
	mockconn "github.com/go-mysql-org/go-mysql/test_util/conn"
	"github.com/stretchr/testify/require"
)

func TestHandleStmtExecute(t *testing.T) {
	c := Conn{}
	c.stmts = map[uint32]*Stmt{
		1: {},
	}
	testcases := []struct {
		data    []byte
		errtext string
	}{
		{
			[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1243 (HY000): Unknown prepared statement handler (0) given to stmt_execute",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flags 0xff",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0x02, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_FOR_UPDATE",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0x04, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_SCROLLABLE",
		},
	}

	for _, tc := range testcases {
		_, err := c.handleStmtExecute(tc.data)
		if tc.errtext == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.errtext)
		}
	}
}

type dupEntryExecHandler struct {
	EmptyHandler
}

func (h dupEntryExecHandler) HandleStmtExecute(any, string, []any) (*mysql.Result, error) {
	return nil, mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry '1' for key 'PRIMARY'")
}

// errors.Trace around HandleStmtExecute must not turn ER_DUP_ENTRY into ER_UNKNOWN_ERROR.
func TestHandleStmtExecuteDupEntryOnWire(t *testing.T) {
	const msg = "Duplicate entry '1' for key 'PRIMARY'"
	c := &Conn{
		h: dupEntryExecHandler{},
		stmts: map[uint32]*Stmt{
			1: {},
		},
	}
	// COM_STMT_EXECUTE: stmt id 1, flags 0, iteration count 1, no params.
	_, err := c.handleStmtExecute([]byte{1, 0, 0, 0, 0, 1, 0, 0, 0})
	require.Error(t, err)

	clientConn := &mockconn.MockConn{}
	out := &Conn{Conn: packet.NewConn(clientConn)}
	out.SetCapability(mysql.CLIENT_PROTOCOL_41)
	require.NoError(t, out.writeError(err))

	payload := append([]byte{
		mysql.ERR_HEADER,
		0x26, 0x04, // 1062
		'#',
		'2', '3', '0', '0', '0',
	}, msg...)
	expected := append([]byte{byte(len(payload)), 0, 0, 0}, payload...)
	require.Equal(t, expected, clientConn.WriteBuffered)
}

type mockPrepareHandler struct {
	EmptyHandler
	context                 any
	paramCount, columnCount int
}

func (h *mockPrepareHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	return h.paramCount, h.columnCount, h.context, nil
}

func TestStmtPrepareWithoutPreparedStmt(t *testing.T) {
	c := &Conn{
		h:     &mockPrepareHandler{context: "plain string", paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT * FROM t"...))

	st := result.(*Stmt)
	require.Nil(t, st.RawParamFields)
	require.Nil(t, st.RawColumnFields)
}

func TestStmtPrepareWithPreparedStmt(t *testing.T) {
	paramField := &mysql.Field{Name: []byte("?"), Type: mysql.MYSQL_TYPE_LONG}
	columnField := &mysql.Field{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONGLONG}

	provider := &stmt.PreparedStmt{
		RawParamFields:  [][]byte{paramField.Dump()},
		RawColumnFields: [][]byte{columnField.Dump()},
	}
	c := &Conn{
		h:     &mockPrepareHandler{context: provider, paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT id FROM t WHERE id = ?"...))

	st := result.(*Stmt)
	require.NotNil(t, st.RawParamFields)
	require.NotNil(t, st.RawColumnFields)
	paramFields, err := st.GetParamFields()
	require.NoError(t, err)
	require.Equal(t, mysql.MYSQL_TYPE_LONG, paramFields[0].Type)
	columnFields, err := st.GetColumnFields()
	require.NoError(t, err)
	require.Equal(t, mysql.MYSQL_TYPE_LONGLONG, columnFields[0].Type)
}

func TestBindStmtArgsTypedBytes(t *testing.T) {
	testcases := []struct {
		name        string
		paramType   byte
		paramValue  []byte
		expectType  byte
		expectBytes []byte
	}{
		{
			name:        "DATETIME",
			paramType:   mysql.MYSQL_TYPE_DATETIME,
			paramValue:  []byte{0x07, 0xe8, 0x07, 0x06, 0x0f, 0x0e, 0x1e, 0x2d},
			expectType:  mysql.MYSQL_TYPE_DATETIME,
			expectBytes: []byte{0xe8, 0x07, 0x06, 0x0f, 0x0e, 0x1e, 0x2d},
		},
		{
			name:        "VARCHAR",
			paramType:   mysql.MYSQL_TYPE_VARCHAR,
			paramValue:  []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
			expectType:  mysql.MYSQL_TYPE_VARCHAR,
			expectBytes: []byte("hello"),
		},
		{
			name:        "BLOB",
			paramType:   mysql.MYSQL_TYPE_BLOB,
			paramValue:  []byte{0x04, 0x00, 0x01, 0x02, 0x03},
			expectType:  mysql.MYSQL_TYPE_BLOB,
			expectBytes: []byte{0x00, 0x01, 0x02, 0x03},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			s := &Stmt{Args: make([]any, 1)}
			s.Params = 1

			nullBitmap := []byte{0x00}
			paramTypes := []byte{tc.paramType, 0x00}

			err := c.bindStmtArgs(s, nullBitmap, paramTypes, tc.paramValue)
			require.NoError(t, err)

			tv, ok := s.Args[0].(mysql.TypedBytes)
			require.True(t, ok, "expected TypedBytes, got %T", s.Args[0])
			require.Equal(t, tc.expectType, tv.Type)
			require.Equal(t, tc.expectBytes, tv.Bytes)
		})
	}
}

// recordingStmtHandler records the arguments of every COM_STMT_EXECUTE and the
// contexts passed to HandleStmtClose.
type recordingStmtHandler struct {
	EmptyHandler
	params   int
	args     [][]any
	err      error
	closeErr error
	closed   []any
	nextCtx  int
}

func (h *recordingStmtHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	h.nextCtx++
	return h.params, 0, h.nextCtx, nil
}

func (h *recordingStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	h.args = append(h.args, slices.Clone(args))
	return nil, h.err
}

func (h *recordingStmtHandler) HandleStmtClose(context any) error {
	h.closed = append(h.closed, context)
	return h.closeErr
}

func newStmtTestConn(h Handler) *Conn {
	return &Conn{h: h, stmts: make(map[uint32]*Stmt)}
}

// executePacket builds a COM_STMT_EXECUTE payload (without the command byte)
// for statement 1.
func executePacket(nullBitmap []byte, types []byte, values []byte) []byte {
	data := []byte{1, 0, 0, 0, mysql.CURSOR_TYPE_NO_CURSOR, 1, 0, 0, 0}
	data = append(data, nullBitmap...)
	if types != nil {
		data = append(data, 1)
		data = append(data, types...)
	} else {
		data = append(data, 0)
	}
	return append(data, values...)
}

func TestStmtExecuteReusesBoundTypes(t *testing.T) {
	h := &recordingStmtHandler{params: 2}
	c := newStmtTestConn(h)
	require.IsType(t, &Stmt{}, c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?, ?"...)))

	types := []byte{mysql.MYSQL_TYPE_LONG, 0, mysql.MYSQL_TYPE_VAR_STRING, 0}
	pkt := executePacket([]byte{0}, types, []byte{7, 0, 0, 0, 2, 'a', 'b'})
	_, err := c.handleStmtExecute(pkt)
	require.NoError(t, err)
	// The server keeps the types, not the packet holding them.
	pkt[11] = mysql.MYSQL_TYPE_TINY

	// Flag 0: the types are not resent, the values are.
	_, err = c.handleStmtExecute(executePacket([]byte{0}, nil, []byte{8, 0, 0, 0, 1, 'c'}))
	require.NoError(t, err)

	// Flag 0 with the second parameter NULL.
	_, err = c.handleStmtExecute(executePacket([]byte{0x02}, nil, []byte{9, 0, 0, 0}))
	require.NoError(t, err)

	require.Equal(t, [][]any{
		{int32(7), mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte("ab")}},
		{int32(8), mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte("c")}},
		{int32(9), nil},
	}, h.args)
}

func TestStmtExecuteWithoutBoundTypes(t *testing.T) {
	h := &recordingStmtHandler{params: 1}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))

	// Every parameter NULL: no types are needed (go-mysql's own client sends
	// this).
	_, err := c.handleStmtExecute(executePacket([]byte{0x01}, nil, nil))
	require.NoError(t, err)
	require.Equal(t, [][]any{{nil}}, h.args)

	// A value with no type ever bound cannot be decoded: MySQL answers
	// ER_WRONG_ARGUMENTS for an inline value and ER_MALFORMED_PACKET for long
	// data.
	_, err = c.handleStmtExecute(executePacket([]byte{0}, nil, []byte{1}))
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_WRONG_ARGUMENTS), myErr.Code)

	c.dispatch(append([]byte{mysql.COM_STMT_SEND_LONG_DATA}, 1, 0, 0, 0, 0, 0, 'x'))
	_, err = c.handleStmtExecute(executePacket([]byte{0}, nil, nil))
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_MALFORMED_PACKET), myErr.Code)
	require.Equal(t, [][]any{{nil}}, h.args, "no execution reaches the handler without types")
}

func TestStmtExecuteRejectedFlagConsumesLongData(t *testing.T) {
	h := &recordingStmtHandler{params: 1}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))

	c.dispatch(append([]byte{mysql.COM_STMT_SEND_LONG_DATA}, 1, 0, 0, 0, 0, 0, 'x'))
	types := []byte{mysql.MYSQL_TYPE_VAR_STRING, 0}
	pkt := executePacket([]byte{0}, types, nil)
	pkt[4] = mysql.CURSOR_TYPE_FOR_UPDATE
	_, err := c.handleStmtExecute(pkt)
	require.Error(t, err)

	// The rejected execution consumed the long data, so this one binds its
	// own inline value.
	_, err = c.handleStmtExecute(executePacket([]byte{0}, types, []byte{1, 'y'}))
	require.NoError(t, err)
	require.Equal(t, [][]any{{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte("y")}}}, h.args)
}

func TestStmtExecuteLongData(t *testing.T) {
	h := &recordingStmtHandler{params: 2}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "INSERT INTO t VALUES (?, ?)"...))

	pkt := []byte{mysql.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 0, 0, 0, 'x', 'y'}
	require.Equal(t, noResponse{}, c.dispatch(pkt))
	// The server keeps the value, not the packet holding it.
	pkt[7] = 'Z'
	c.dispatch(append([]byte{mysql.COM_STMT_SEND_LONG_DATA}, 1, 0, 0, 0, 0, 0, 'z'))

	// Parameter 0 has long data, so only parameter 1's value is in the
	// packet, and parameter 0's NULL bit is ignored.
	types := []byte{mysql.MYSQL_TYPE_BLOB, 0, mysql.MYSQL_TYPE_TINY, 0}
	_, err := c.handleStmtExecute(executePacket([]byte{0x01}, types, []byte{5}))
	require.NoError(t, err)

	// Long data is consumed by the execution.
	_, err = c.handleStmtExecute(executePacket([]byte{0}, nil, []byte{1, 'q', 6}))
	require.NoError(t, err)

	require.Equal(t, [][]any{
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("xyz")}, int8(5)},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("q")}, int8(6)},
	}, h.args)
}

func TestStmtExecuteErrorKeepsCode(t *testing.T) {
	h := &recordingStmtHandler{err: mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry '1' for key 'PRIMARY'")}
	clientConn := &mockconn.MockConn{}
	c := newStmtTestConn(h)
	c.Conn = packet.NewConn(clientConn)
	c.SetCapability(mysql.CLIENT_PROTOCOL_41)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "INSERT INTO t VALUES (1)"...))
	clientConn.WriteBuffered = nil

	v := c.dispatch(append([]byte{mysql.COM_STMT_EXECUTE}, executePacket(nil, nil, nil)[:9]...))
	require.NoError(t, c.WriteValue(v))
	require.NoError(t, c.Flush())
	payload := clientConn.WriteBuffered[4:]
	require.Equal(t, mysql.ERR_HEADER, payload[0])
	require.Equal(t, uint16(mysql.ER_DUP_ENTRY), uint16(payload[1])|uint16(payload[2])<<8)
	require.Equal(t, "#23000", string(payload[3:9]))
}

func TestResetStmts(t *testing.T) {
	h := &recordingStmtHandler{}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT 1"...))
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT 2"...))

	require.NoError(t, c.ResetStmts())
	require.ElementsMatch(t, []any{1, 2}, h.closed)

	_, err := c.handleStmtExecute([]byte{1, 0, 0, 0, 0, 1, 0, 0, 0})
	require.ErrorContains(t, err, "Unknown prepared statement handler (1)")
}

func TestStmtSendLongDataEmptyChunk(t *testing.T) {
	h := &recordingStmtHandler{params: 2}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "INSERT INTO t VALUES (?, ?)"...))

	// An empty chunk still marks parameter 0 as sent through long data, so
	// the packet holds only parameter 1's value.
	c.dispatch([]byte{mysql.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 0, 0, 0})
	types := []byte{mysql.MYSQL_TYPE_BLOB, 0, mysql.MYSQL_TYPE_TINY, 0}
	_, err := c.handleStmtExecute(executePacket([]byte{0}, types, []byte{5}))
	require.NoError(t, err)
	require.Equal(t, [][]any{{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte{}}, int8(5)}}, h.args)
}

func TestStmtExecuteLongDataRequiresStringType(t *testing.T) {
	// MySQL 8.0 accepts long data only for the string and blob types and
	// answers ER_MALFORMED_PACKET for any other declared type.
	for _, tp := range []byte{
		mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB,
		mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING,
	} {
		h := &recordingStmtHandler{params: 1}
		c := newStmtTestConn(h)
		c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))
		c.dispatch([]byte{mysql.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 0, 0, 0, '4', '2'})
		_, err := c.handleStmtExecute(executePacket([]byte{0}, []byte{tp, 0}, nil))
		require.NoError(t, err, "type %d", tp)
		require.Equal(t, [][]any{{mysql.TypedBytes{Type: tp, Bytes: []byte("42")}}}, h.args)
	}

	for _, tp := range []byte{
		mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_DATETIME,
		mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_JSON, mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_GEOMETRY,
	} {
		h := &recordingStmtHandler{params: 2}
		c := newStmtTestConn(h)
		c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?, ?"...))
		c.dispatch([]byte{mysql.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 0, 0, 0, '4', '2'})
		types := []byte{tp, 0, mysql.MYSQL_TYPE_TINY, 0}
		_, err := c.handleStmtExecute(executePacket([]byte{0}, types, []byte{5}))
		var myErr *mysql.MyError
		require.ErrorAs(t, err, &myErr, "type %d", tp)
		require.Equal(t, uint16(mysql.ER_MALFORMED_PACKET), myErr.Code, "type %d", tp)
		require.Empty(t, h.args)
	}
}

func TestStmtMalformedPacketCodes(t *testing.T) {
	h := &recordingStmtHandler{params: 1}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))
	long := []byte{mysql.MYSQL_TYPE_LONG, 0}
	str := []byte{mysql.MYSQL_TYPE_VAR_STRING, 0}

	// Codes measured against MySQL 8.0.44.
	for _, tc := range []struct {
		name string
		run  func() error
		code uint16
	}{
		{"execute without parameters block", func() error {
			_, err := c.handleStmtExecute([]byte{1, 0, 0, 0, 0})
			return err
		}, mysql.ER_MALFORMED_PACKET},
		{"execute without null bitmap", func() error {
			_, err := c.handleStmtExecute(executePacket(nil, nil, nil)[:9])
			return err
		}, mysql.ER_MALFORMED_PACKET},
		{"truncated types", func() error {
			_, err := c.handleStmtExecute(executePacket([]byte{0}, long[:1], nil))
			return err
		}, mysql.ER_MALFORMED_PACKET},
		{"truncated fixed-width value", func() error {
			_, err := c.handleStmtExecute(executePacket([]byte{0}, long, []byte{1, 2}))
			return err
		}, mysql.ER_MALFORMED_PACKET},
		{"truncated length-encoded value", func() error {
			_, err := c.handleStmtExecute(executePacket([]byte{0}, str, []byte{5, 'a'}))
			return err
		}, mysql.ER_MALFORMED_PACKET},
		{"unknown type", func() error {
			_, err := c.handleStmtExecute(executePacket([]byte{0}, []byte{0x14, 0}, []byte{1, 'a'}))
			return err
		}, mysql.ER_WRONG_ARGUMENTS},
		{"short reset", func() error {
			_, err := c.handleStmtReset([]byte{1, 0})
			return err
		}, mysql.ER_MALFORMED_PACKET},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var myErr *mysql.MyError
			require.ErrorAs(t, tc.run(), &myErr)
			require.Equal(t, tc.code, myErr.Code)
		})
	}
	require.Empty(t, h.args)
}

func TestResetStmtsReportsCloseErrors(t *testing.T) {
	closeErr := stderrors.New("close failed")
	h := &recordingStmtHandler{closeErr: closeErr}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT 1"...))
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT 2"...))

	err := c.ResetStmts()
	require.ErrorIs(t, err, closeErr)
	require.ElementsMatch(t, []any{1, 2}, h.closed)
	require.Empty(t, c.stmts, "statements are deallocated even when a close fails")
}

func TestStmtCloseDeallocatesWhenTheHandlerFails(t *testing.T) {
	closeErr := stderrors.New("close failed")
	h := &accountingStmtHandler{recordingStmtHandler: recordingStmtHandler{params: 1, closeErr: closeErr}, limit: 6}
	c := newStmtTestConn(h)
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))
	c.dispatch(longDataPacket(0, "abc"))
	require.Equal(t, 3, h.reserved)

	require.ErrorIs(t, c.handleStmtClose([]byte{1, 0, 0, 0}), closeErr)
	require.Empty(t, c.stmts, "the statement is deallocated even when its close fails")
	require.Equal(t, 0, h.reserved, "its long data is released")
}

// longDataPacket is a COM_STMT_SEND_LONG_DATA for statement 1.
func longDataPacket(param byte, chunk string) []byte {
	return append([]byte{mysql.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 0, param, 0}, chunk...)
}

func TestStmtLongDataLimit(t *testing.T) {
	h := &recordingStmtHandler{params: 2}
	c := newStmtTestConn(h)
	c.serverConf = &Server{maxLongDataSize: 4}
	c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "INSERT INTO t VALUES (?, ?)"...))
	types := []byte{mysql.MYSQL_TYPE_BLOB, 0, mysql.MYSQL_TYPE_BLOB, 0}

	// The bound is per parameter and inclusive: 4 bytes in each is accepted.
	c.dispatch(longDataPacket(0, "ab"))
	c.dispatch(longDataPacket(0, "cd"))
	c.dispatch(longDataPacket(1, "efgh"))
	_, err := c.handleStmtExecute(executePacket([]byte{0}, types, nil))
	require.NoError(t, err)

	// One byte past it puts the statement in the error state.
	c.dispatch(longDataPacket(0, "abc"))
	c.dispatch(longDataPacket(0, "de"))
	tooLong := "Parameter of prepared statement which is set through mysql_send_long_data() is longer than 'max_allowed_packet' bytes"
	for range 2 {
		_, err = c.handleStmtExecute(executePacket([]byte{0}, types, []byte{1, 'x', 1, 'y'}))
		var myErr *mysql.MyError
		require.ErrorAs(t, err, &myErr)
		require.Equal(t, uint16(mysql.ER_UNKNOWN_ERROR), myErr.Code)
		require.Equal(t, "HY000", myErr.State)
		require.Equal(t, tooLong, myErr.Message)
	}
	// Long data is ignored in the error state, and the refused bytes are
	// not kept.
	c.dispatch(longDataPacket(1, "z"))
	require.Nil(t, c.stmts[1].longData)
	_, err = c.handleStmtExecute(executePacket([]byte{0}, types, nil))
	require.ErrorContains(t, err, tooLong)

	// COM_STMT_RESET clears it.
	_, err = c.handleStmtReset([]byte{1, 0, 0, 0})
	require.NoError(t, err)
	_, err = c.handleStmtExecute(executePacket([]byte{0}, types, []byte{1, 'x', 1, 'y'}))
	require.NoError(t, err)

	require.Equal(t, [][]any{
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("abcd")}, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("efgh")}},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("x")}, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("y")}},
	}, h.args)
}

func TestSetMaxLongDataSize(t *testing.T) {
	s := &Server{}
	s.SetMaxLongDataSize(10)
	require.Equal(t, 10, s.MaxLongDataSize())
	s.SetMaxLongDataSize(0)
	require.Equal(t, DefaultMaxLongDataSize, s.MaxLongDataSize())
	// A connection without a server configuration has the default bound.
	require.Equal(t, DefaultMaxLongDataSize, newStmtTestConn(&recordingStmtHandler{}).maxLongDataSize())
}

// TestSetMaxLongDataSizeWhileServing changes the bound while a connection
// reads it; run with -race.
func TestSetMaxLongDataSizeWhileServing(t *testing.T) {
	s := &Server{}
	c := newStmtTestConn(&recordingStmtHandler{})
	c.serverConf = s
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 1000; i++ {
			s.SetMaxLongDataSize(i)
		}
	}()
	for range 1000 {
		require.Positive(t, c.maxLongDataSize())
	}
	<-done
	require.Equal(t, 1000, c.maxLongDataSize())
}

// accountingStmtHandler records long data reservations and refuses those
// that would take the total past limit.
type accountingStmtHandler struct {
	recordingStmtHandler
	limit    int
	reserved int
}

func (h *accountingStmtHandler) ReserveLongData(n int) error {
	if h.reserved+n > h.limit {
		return mysql.NewDefaultError(mysql.ER_OUT_OF_RESOURCES)
	}
	h.reserved += n
	return nil
}

func (h *accountingStmtHandler) ReleaseLongData(n int) {
	h.reserved -= n
}

func TestStmtLongDataHandler(t *testing.T) {
	h := &accountingStmtHandler{recordingStmtHandler: recordingStmtHandler{params: 1}, limit: 6}
	c := newStmtTestConn(h)
	prepare := func() {
		c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT ?"...))
	}
	types := []byte{mysql.MYSQL_TYPE_BLOB, 0}
	prepare()

	// Consumed by an execution.
	for _, chunk := range []string{"abc", "a"} {
		c.dispatch(longDataPacket(0, chunk))
		require.Equal(t, len(chunk), h.reserved)
		_, err := c.handleStmtExecute(executePacket([]byte{0}, types, nil))
		require.NoError(t, err)
		require.Equal(t, 0, h.reserved)
	}

	// Cleared by COM_STMT_RESET.
	c.dispatch(longDataPacket(0, "abc"))
	_, err := c.handleStmtReset([]byte{1, 0, 0, 0})
	require.NoError(t, err)
	require.Equal(t, 0, h.reserved)

	// Refused: the handler's error is the statement's error state, and what
	// was reserved before is released.
	c.dispatch(longDataPacket(0, "abcd"))
	c.dispatch(longDataPacket(0, "efg"))
	require.Equal(t, 0, h.reserved)
	_, err = c.handleStmtExecute(executePacket([]byte{0}, types, nil))
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_OUT_OF_RESOURCES), myErr.Code)
	_, err = c.handleStmtReset([]byte{1, 0, 0, 0})
	require.NoError(t, err)

	// Dropped with the statement by COM_STMT_CLOSE.
	c.dispatch(longDataPacket(0, "abc"))
	require.NoError(t, c.handleStmtClose([]byte{1, 0, 0, 0}))
	require.Equal(t, 0, h.reserved)

	// Dropped by ResetStmts.
	prepare()
	c.dispatch(append([]byte{mysql.COM_STMT_SEND_LONG_DATA, 2, 0, 0, 0, 0, 0}, "abc"...))
	require.Equal(t, 3, h.reserved)
	require.NoError(t, c.ResetStmts())
	require.Equal(t, 0, h.reserved)
}

// sharedQuotaHandler is one handler shared by every connection, accounting
// long data against a single quota, the use case LongDataHandler documents.
type sharedQuotaHandler struct {
	EmptyHandler
	mu       sync.Mutex
	limit    int
	reserved int
}

func (h *sharedQuotaHandler) HandleStmtPrepare(string) (int, int, any, error) {
	return 1, 0, nil, nil
}

func (h *sharedQuotaHandler) HandleStmtClose(any) error { return nil }

func (h *sharedQuotaHandler) ReserveLongData(n int) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reserved+n > h.limit {
		return mysql.NewDefaultError(mysql.ER_OUT_OF_RESOURCES)
	}
	h.reserved += n
	return nil
}

func (h *sharedQuotaHandler) ReleaseLongData(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reserved -= n
}

func (h *sharedQuotaHandler) held() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reserved
}

// TestLongDataReleasedOnTeardown buffers long data against a shared quota,
// ends the connection without executing, resetting or closing the
// statement, and checks a new connection can use the whole quota: after the
// client disconnects (a read error), after COM_QUIT, and after the server
// calls Conn.Close between commands, with no HandleCommand after it.
func TestLongDataReleasedOnTeardown(t *testing.T) {
	const quota = 6
	for _, teardown := range []string{"client disconnects", "client quits", "server closes"} {
		t.Run(teardown, func(t *testing.T) {
			h := &sharedQuotaHandler{limit: quota}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			serverConns := make(chan *Conn, 2)
			// closeAfterCommand makes the first connection's command loop
			// call Close after the next command and stop.
			var closeAfterCommand atomic.Bool
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := range 2 {
					conn, err := l.Accept()
					if err != nil {
						return
					}
					auth := NewInMemoryAuthenticationHandler()
					if err := auth.AddUser("u1", "p1"); err != nil {
						return
					}
					co, err := NewDefaultServer().NewCustomizedConn(conn, auth, h)
					if err != nil {
						return
					}
					serverConns <- co
					go func() {
						for co.HandleCommand() == nil {
							if i == 0 && closeAfterCommand.Load() {
								co.Close()
								return
							}
						}
					}()
				}
			}()
			t.Cleanup(func() {
				l.Close()
				<-done
			})

			// fill prepares a statement and buffers quota bytes of long data.
			fill := func() *client.Conn {
				c, err := client.Connect(l.Addr().String(), "u1", "p1", "")
				require.NoError(t, err)
				stmt, err := c.Prepare("SELECT ?")
				require.NoError(t, err)
				c.ResetSequence()
				pkt := append([]byte{0, 0, 0, 0, mysql.COM_STMT_SEND_LONG_DATA}, mysql.Uint32ToBytes(stmt.ID)...)
				pkt = append(pkt, 0, 0)
				pkt = append(pkt, strings.Repeat("x", quota)...)
				require.NoError(t, c.WritePacket(pkt))
				// COM_STMT_SEND_LONG_DATA has no response; a ping's answer
				// means it has been handled.
				require.NoError(t, c.Ping())
				return c
			}

			first := fill()
			<-serverConns
			require.Equal(t, quota, h.held())
			switch teardown {
			case "client disconnects":
				require.NoError(t, first.Close())
			case "client quits":
				require.NoError(t, first.Quit())
			case "server closes":
				closeAfterCommand.Store(true)
				require.NoError(t, first.Ping())
				t.Cleanup(func() { first.Close() })
			}
			require.Eventually(t, func() bool { return h.held() == 0 }, 5*time.Second, time.Millisecond)

			second := fill()
			t.Cleanup(func() { second.Close() })
			<-serverConns
			require.Equal(t, quota, h.held(), "the new connection has the whole quota")
		})
	}
}

// Clients only send the parameter types (new-params-bound = 1) when they change,
// so later executions must decode their values with the previously sent types.
func TestHandleStmtExecuteReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}

	// Types sent: LONGLONG, value 2.
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_LONGLONG, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, value 7.
	require.NoError(t, execute(0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, NULL value.
	require.NoError(t, execute(0x1, 0x0))

	require.Equal(t, [][]any{{int64(2)}, {int64(7)}, {nil}}, h.args)
}

func TestHandleStmtExecuteWithoutParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	// The first execution doesn't send the types, so the value can't be decoded.
	data := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	_, err := c.handleStmtExecute(data)
	require.ErrorContains(t, err, "Incorrect arguments to mysqld_stmt_execute")
	require.Empty(t, h.args)
}

// A first execution whose parameters are all NULL needs no types, and the go-mysql
// client sends none for it (new-params-bound = 0).
func TestHandleStmtExecuteFirstAllNullWithoutParamTypes(t *testing.T) {
	for _, params := range []int{1, 2, 9} {
		h := &recordingStmtHandler{}
		s := &Stmt{}
		s.Rest(params, 0, nil)
		c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

		nullBitmap := make([]byte, (params+7)>>3)
		for i := range params {
			nullBitmap[i>>3] |= 1 << (uint(i) % 8)
		}
		data := append([]byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}, nullBitmap...)
		_, err := c.handleStmtExecute(append(data, 0x0))
		require.NoError(t, err)
		require.Equal(t, [][]any{make([]any, params)}, h.args)
	}
}

func TestHandleStmtExecuteMixedNullWithoutParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(2, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	// The first parameter is NULL, the second can't be decoded without its type.
	data := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	_, err := c.handleStmtExecute(data)
	require.ErrorContains(t, err, "Incorrect arguments to mysqld_stmt_execute")
	require.Empty(t, h.args)
}

func TestHandleStmtExecuteAllNullThenReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}

	// NULL value, no types sent.
	require.NoError(t, execute(0x1, 0x0))
	// Types sent: LONGLONG, value 5.
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_LONGLONG, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, value 7.
	require.NoError(t, execute(0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))

	require.Equal(t, [][]any{{nil}, {int64(5)}, {int64(7)}}, h.args)
}

// Long-data parameters have no inline value in COM_STMT_EXECUTE, also when the
// types sent by an earlier execution are reused (new-params-bound = 0).
func TestHandleStmtExecuteLongDataReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}
	sendLongData := func(chunk string) {
		data := append([]byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0}, chunk...) // stmt id, param id
		require.NoError(t, c.handleStmtSendLongData(data))
	}

	// Types sent: BLOB, inline value "x".
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_BLOB, 0x0, 0x1, 'x'))
	// Types not sent again, no inline value for the long-data parameter.
	sendLongData("lo")
	sendLongData("ng")
	require.NoError(t, execute(0x0, 0x0))
	// The long data is consumed by the execution above.
	require.NoError(t, execute(0x0, 0x0, 0x1, 'y'))
	// Types sent again, still no inline value for the long-data parameter.
	sendLongData("again")
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_BLOB, 0x0))

	require.Equal(t, [][]any{
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("x")}},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("long")}},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("y")}},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("again")}},
	}, h.args)
}

type prepareStmtHandler struct {
	recordingStmtHandler
}

func (h *prepareStmtHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	return 1, 0, nil, nil
}

func (h *prepareStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	h.args = append(h.args, append([]any(nil), args...))
	return &mysql.Result{}, nil
}

func TestClientStmtExecuteNullFirst(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	h := &prepareStmtHandler{}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		co, _ := NewConn(conn, "root", "", h)
		if co != nil {
			//nolint:revive // loop drains commands; work is in the condition
			for co.HandleCommand() == nil {
			}
		}
	}()

	conn, err := client.Connect(l.Addr().String(), "root", "", "")
	require.NoError(t, err)
	defer conn.Close()

	st, err := conn.Prepare("SELECT ?")
	require.NoError(t, err)
	defer st.Close()

	_, err = st.Execute(nil)
	require.NoError(t, err)
	_, err = st.Execute(int64(5))
	require.NoError(t, err)
	_, err = st.Execute(nil)
	require.NoError(t, err)

	require.Equal(t, [][]any{{nil}, {int64(5)}, {nil}}, h.args)
}

type cursorStmtHandler struct {
	EmptyHandler
	rs *mysql.Resultset
}

func (h *cursorStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	var err error
	h.rs, err = mysql.BuildSimpleBinaryResultset([]string{"id", "name"},
		[][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(h.rs), nil
}

// splitPackets decodes the packets of a MySQL packet stream.
func splitPackets(t *testing.T, buf []byte) [][]byte {
	t.Helper()
	var packets [][]byte
	for len(buf) > 0 {
		require.GreaterOrEqual(t, len(buf), 4)
		n := int(buf[0]) | int(buf[1])<<8 | int(buf[2])<<16
		require.GreaterOrEqual(t, len(buf), 4+n)
		packets = append(packets, buf[4:4+n])
		buf = buf[4+n:]
	}
	return packets
}

// statusOf returns the status flags of an EOF packet, or of the EOF-headed OK
// packet sent instead of it under CLIENT_DEPRECATE_EOF.
func statusOf(t *testing.T, packet []byte) uint16 {
	t.Helper()
	require.Equal(t, mysql.EOF_HEADER, packet[0])
	return uint16(packet[3]) | uint16(packet[4])<<8
}

// A COM_STMT_EXECUTE asking for CURSOR_TYPE_READ_ONLY answers with the column
// definitions and SERVER_STATUS_CURSOR_EXISTS only; COM_STMT_FETCH then serves
// the rows in batches and ends the cursor with SERVER_STATUS_LAST_ROW_SEND.
func TestStmtCursor(t *testing.T) {
	for _, deprecateEOF := range []bool{false, true} {
		t.Run(fmt.Sprintf("deprecateEOF=%t", deprecateEOF), func(t *testing.T) {
			h := &cursorStmtHandler{}
			clientConn := &mockconn.MockConn{MultiWrite: true}
			c := &Conn{
				Conn:       packet.NewConn(clientConn),
				serverConf: &Server{},
				capability: mysql.CLIENT_PROTOCOL_41,
				h:          h,
				stmts:      make(map[uint32]*Stmt),
			}
			if deprecateEOF {
				c.SetCapability(mysql.CLIENT_DEPRECATE_EOF)
				require.NoError(t, c.serverConf.SetCapability(mysql.CLIENT_DEPRECATE_EOF))
			}

			st := &Stmt{}
			st.Rest(0, 2, nil)
			c.stmts[1] = st

			// run dispatches one command and returns the packets it answered with.
			run := func(cmd byte, args ...byte) [][]byte {
				clientConn.WriteBuffered = nil
				require.NoError(t, c.WriteValue(c.dispatch(append([]byte{cmd}, args...))))
				return splitPackets(t, clientConn.WriteBuffered)
			}
			// COM_STMT_EXECUTE: stmt id 1, flags, iteration count 1, no params.
			execute := func(flags byte) [][]byte {
				return run(mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, flags, 1, 0, 0, 0)
			}
			// COM_STMT_FETCH: stmt id 1, num_rows.
			fetch := func(numRows byte) [][]byte {
				return run(mysql.COM_STMT_FETCH, 1, 0, 0, 0, numRows, 0, 0, 0)
			}

			// The cursor execution: column count, the column definitions, and a
			// terminator carrying SERVER_STATUS_CURSOR_EXISTS. No rows.
			packets := execute(mysql.CURSOR_TYPE_READ_ONLY)
			require.Len(t, packets, 4)
			require.Equal(t, []byte{2}, packets[0])
			require.Equal(t, h.rs.Fields[0].Dump(), packets[1])
			require.Equal(t, h.rs.Fields[1].Dump(), packets[2])
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[3]))

			// The cursor drains the row buffer of h.rs, so keep a copy of it.
			var rows [][]byte
			for _, row := range h.rs.RowDatas {
				rows = append(rows, row)
			}

			// A batch smaller than the cursor: the cursor stays open.
			packets = fetch(2)
			require.Len(t, packets, 3)
			require.Equal(t, rows[0], packets[0])
			require.Equal(t, rows[1], packets[1])
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[2]))

			// A zero-count fetch asks for no rows: an empty batch, the cursor
			// still open, and the remaining rows served afterwards.
			packets = fetch(0)
			require.Len(t, packets, 1)
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[0]))
			require.NotNil(t, st.cursor)

			// The last row is sent with SERVER_STATUS_LAST_ROW_SEND.
			packets = fetch(2)
			require.Len(t, packets, 2)
			require.Equal(t, rows[2], packets[0])
			require.Equal(t, mysql.SERVER_STATUS_LAST_ROW_SEND, statusOf(t, packets[1]))

			// The flags describe this response only: the next command answers
			// without them.
			packets = run(mysql.COM_PING)
			require.Len(t, packets, 1)
			require.Zero(t, uint16(packets[0][3])|uint16(packets[0][4])<<8)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_LAST_ROW_SEND))

			// A drained cursor is closed, so fetching again is an error rather
			// than an empty batch that never ends.
			packets = fetch(1)
			require.Len(t, packets, 1)
			require.Equal(t, mysql.ERR_HEADER, packets[0][0])
			require.Equal(t, uint16(mysql.ER_STMT_HAS_NO_OPEN_CURSOR), binary.LittleEndian.Uint16(packets[0][1:3]))

			// An execution without the cursor flag answers inline again, and
			// closes the cursor left open by the previous one.
			packets = execute(mysql.CURSOR_TYPE_NO_CURSOR)
			require.Equal(t, mysql.EOF_HEADER, packets[len(packets)-1][0])
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_LAST_ROW_SEND))
			require.Nil(t, st.cursor)

			// COM_STMT_RESET closes the cursor.
			execute(mysql.CURSOR_TYPE_READ_ONLY)
			require.True(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			run(mysql.COM_STMT_RESET, 1, 0, 0, 0)
			require.Nil(t, st.cursor)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			_, err := c.handleStmtFetch([]byte{1, 0, 0, 0, 1, 0, 0, 0})
			require.ErrorContains(t, err, "has no open cursor")

			// COM_STMT_CLOSE drops the cursor with the statement.
			execute(mysql.CURSOR_TYPE_READ_ONLY)
			run(mysql.COM_STMT_CLOSE, 1, 0, 0, 0)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			_, err = c.handleStmtFetch([]byte{1, 0, 0, 0, 1, 0, 0, 0})
			require.ErrorContains(t, err, "Unknown prepared statement handler")
		})
	}
}

// With cursors disabled, CURSOR_TYPE_READ_ONLY is answered like
// CURSOR_TYPE_NO_CURSOR: the rows inline, no SERVER_STATUS_CURSOR_EXISTS, and
// nothing kept on the statement.
func TestStmtCursorDisabled(t *testing.T) {
	for _, deprecateEOF := range []bool{false, true} {
		t.Run(fmt.Sprintf("deprecateEOF=%t", deprecateEOF), func(t *testing.T) {
			h := &cursorStmtHandler{}
			clientConn := &mockconn.MockConn{MultiWrite: true}
			c := &Conn{
				Conn:       packet.NewConn(clientConn),
				serverConf: &Server{},
				capability: mysql.CLIENT_PROTOCOL_41,
				h:          h,
				stmts:      make(map[uint32]*Stmt),
			}
			require.True(t, c.serverConf.CursorsEnabled())
			c.serverConf.SetCursorsEnabled(false)
			require.False(t, c.serverConf.CursorsEnabled())
			if deprecateEOF {
				c.SetCapability(mysql.CLIENT_DEPRECATE_EOF)
				require.NoError(t, c.serverConf.SetCapability(mysql.CLIENT_DEPRECATE_EOF))
			}
			st := &Stmt{}
			st.Rest(0, 2, nil)
			c.stmts[1] = st

			require.NoError(t, c.WriteValue(c.dispatch([]byte{mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, mysql.CURSOR_TYPE_READ_ONLY, 1, 0, 0, 0})))
			packets := splitPackets(t, clientConn.WriteBuffered)

			// Column count, two definitions, the EOF after them unless
			// deprecated, three rows, and the terminator.
			want := 1 + 2 + 3 + 1
			if !deprecateEOF {
				want++
			}
			require.Len(t, packets, want)
			require.Equal(t, []byte{2}, packets[0])
			require.Zero(t, statusOf(t, packets[len(packets)-1])&mysql.SERVER_STATUS_CURSOR_EXISTS)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			require.Nil(t, st.cursor)

			_, err := c.handleStmtFetch([]byte{1, 0, 0, 0, 1, 0, 0, 0})
			require.ErrorContains(t, err, "has no open cursor")
		})
	}
}

// newCursorTestConn returns a connection with statement 1 prepared, serving
// cursorStmtHandler's three rows.
func newCursorTestConn() (*Conn, *mockconn.MockConn, *Stmt) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	c := &Conn{
		Conn:       packet.NewConn(clientConn),
		serverConf: &Server{},
		capability: mysql.CLIENT_PROTOCOL_41,
		h:          &cursorStmtHandler{},
		stmts:      make(map[uint32]*Stmt),
	}
	st := &Stmt{}
	st.Rest(0, 2, nil)
	c.stmts[1] = st
	return c, clientConn, st
}

// Cursors are enabled on a new Server, and on a Conn that has no Server.
func TestStmtCursorsEnabledByDefault(t *testing.T) {
	require.True(t, (&Server{}).CursorsEnabled())
	require.True(t, (&Conn{}).cursorsEnabled())
}

// Disabling cursors affects only executions that start after it: a cursor
// opened before stays fetchable until the statement is executed again.
func TestStmtCursorOpenedBeforeDisable(t *testing.T) {
	c, clientConn, st := newCursorTestConn()
	run := func(data ...byte) [][]byte {
		t.Helper()
		clientConn.WriteBuffered = nil
		require.NoError(t, c.WriteValue(c.dispatch(data)))
		return splitPackets(t, clientConn.WriteBuffered)
	}
	execute := []byte{mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, mysql.CURSOR_TYPE_READ_ONLY, 1, 0, 0, 0}
	fetchOne := []byte{mysql.COM_STMT_FETCH, 1, 0, 0, 0, 1, 0, 0, 0}

	run(execute...)
	require.NotNil(t, st.cursor)

	c.serverConf.SetCursorsEnabled(false)
	// One row and the EOF: the earlier cursor is still served.
	require.Len(t, run(fetchOne...), 2)
	require.NotNil(t, st.cursor)

	// Executing again answers inline and drops the cursor.
	run(execute...)
	require.Nil(t, st.cursor)
	packets := run(fetchOne...)
	require.Len(t, packets, 1)
	require.Equal(t, mysql.ERR_HEADER, packets[0][0])

	c.serverConf.SetCursorsEnabled(true)
	run(execute...)
	require.NotNil(t, st.cursor)
}

// SetCursorsEnabled may be called while executions are served. Meaningful
// under -race.
func TestStmtSetCursorsEnabledConcurrently(t *testing.T) {
	c, clientConn, _ := newCursorTestConn()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			c.serverConf.SetCursorsEnabled(i%2 == 0)
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			clientConn.WriteBuffered = nil
			_ = c.WriteValue(c.dispatch([]byte{mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, mysql.CURSOR_TYPE_READ_ONLY, 1, 0, 0, 0}))
		}
	}()
	wg.Wait()
}

type noResultStmtHandler struct {
	EmptyHandler
}

func (h noResultStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	return mysql.NewResultReserveResultset(0), nil
}

// CURSOR_TYPE_READ_ONLY on a statement that has no result set is a plain OK.
func TestStmtCursorWithoutResultset(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	c := &Conn{
		Conn:       packet.NewConn(clientConn),
		serverConf: &Server{},
		capability: mysql.CLIENT_PROTOCOL_41,
		h:          noResultStmtHandler{},
		stmts:      map[uint32]*Stmt{1: {}},
	}

	require.NoError(t, c.WriteValue(c.dispatch([]byte{mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, mysql.CURSOR_TYPE_READ_ONLY, 1, 0, 0, 0})))

	packets := splitPackets(t, clientConn.WriteBuffered)
	require.Len(t, packets, 1)
	require.Equal(t, mysql.OK_HEADER, packets[0][0])
	require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
}
