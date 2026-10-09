package server

import (
	"slices"
	"testing"

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
			[]byte{0x1, 0x0, 0x0, 0x0, 0x01, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_READ_ONLY",
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
	params  int
	args    [][]any
	err     error
	closed  []any
	nextCtx int
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
	return nil
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
	_, err := c.handleStmtExecute(executePacket([]byte{0}, types, []byte{7, 0, 0, 0, 2, 'a', 'b'}))
	require.NoError(t, err)

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
	pkt[4] = mysql.CURSOR_TYPE_READ_ONLY
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

	chunk := []byte{1, 0, 0, 0, 0, 0, 'x', 'y'}
	require.Equal(t, noResponse{}, c.dispatch(append([]byte{mysql.COM_STMT_SEND_LONG_DATA}, chunk...)))
	// The packet buffer is reused by the next read; the server must have
	// copied it.
	chunk[6] = 'Z'
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
