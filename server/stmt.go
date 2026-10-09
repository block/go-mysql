package server

import (
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"math"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/stmt"
	"github.com/pingcap/errors"
)

var (
	paramFieldData  = (&mysql.Field{Name: []byte("?")}).Dump()
	columnFieldData = (&mysql.Field{}).Dump()
)

type Stmt struct {
	Query string
	Args  []any

	Context any

	// PreparedStmt contains common fields shared with client.Stmt for proxy passthrough
	stmt.PreparedStmt

	// paramTypes holds the type-and-flag pairs (2 bytes per parameter) the
	// client last bound. A COM_STMT_EXECUTE whose new-params-bound flag is 0
	// still sends parameter values, encoded with these types; libmysqlclient
	// and Connector/J do this on every execution after the first.
	paramTypes []byte
	// longData holds the bytes accumulated by COM_STMT_SEND_LONG_DATA per
	// parameter. A parameter with long data has no value in the
	// COM_STMT_EXECUTE packet. Cleared after every execution and by
	// COM_STMT_RESET.
	longData [][]byte
	// longDataSize is the total of longData, reserved through the handler's
	// LongDataHandler when it has one.
	longDataSize int
	// longDataErr is the statement's error state: set when long data is
	// refused, returned by every COM_STMT_EXECUTE, cleared only by
	// COM_STMT_RESET, as in MySQL.
	longDataErr error
}

// LongDataHandler is implemented by a Handler that accounts for the memory
// COM_STMT_SEND_LONG_DATA retains, for instance against a bound shared by
// every connection. ReserveLongData is called before n more bytes are
// retained; an error refuses them and puts the statement in the error state
// SetMaxLongDataSize describes, with that error. ReleaseLongData returns
// bytes when they are discarded: consumed by an execution, cleared by
// COM_STMT_RESET, refused, or dropped with the statement by COM_STMT_CLOSE
// or Conn.ResetStmts, and everything still held when the connection is
// closed (Conn.Close, which every teardown in HandleCommand calls). Each
// reserved byte is released exactly once. Neither method is called with a
// lock held, so either may call Conn.Close.
type LongDataHandler interface {
	ReserveLongData(n int) error
	ReleaseLongData(n int)
}

func (s *Stmt) Rest(params int, columns int, context any) {
	s.Params = params
	s.Columns = columns
	s.Context = context
	s.ResetParams()
}

// ResetParams clears the bound values and long data. It does not release
// long data reserved through a LongDataHandler; the server's own resets do.
func (s *Stmt) ResetParams() {
	s.Args = make([]any, s.Params)
	s.longData = nil
	s.longDataSize = 0
}

// resetStmtParams is ResetParams, releasing the statement's long data to the
// handler's LongDataHandler.
func (c *Conn) resetStmtParams(s *Stmt) {
	c.releaseLongData(s.longDataSize)
	s.ResetParams()
}

// errLongDataConnClosed refuses long data that arrives on a closed connection.
var errLongDataConnClosed = stderrors.New("connection closed")

// reserveLongData reserves n bytes through the handler's LongDataHandler and
// counts them as held by this connection. Once the connection is closed it
// refuses, returning anything the handler just granted.
func (c *Conn) reserveLongData(n int) error {
	h, ok := c.h.(LongDataHandler)
	if !ok || n == 0 {
		return nil
	}
	if err := h.ReserveLongData(n); err != nil {
		return err
	}
	c.longDataMu.Lock()
	if c.longDataReleased {
		c.longDataMu.Unlock()
		h.ReleaseLongData(n)
		return errLongDataConnClosed
	}
	c.longDataHeld += n
	c.longDataMu.Unlock()
	return nil
}

// releaseLongData returns n held bytes to the handler, unless Close already
// returned everything.
func (c *Conn) releaseLongData(n int) {
	h, ok := c.h.(LongDataHandler)
	if !ok || n == 0 {
		return
	}
	c.longDataMu.Lock()
	if c.longDataReleased {
		c.longDataMu.Unlock()
		return
	}
	c.longDataHeld -= n
	c.longDataMu.Unlock()
	h.ReleaseLongData(n)
}

// releaseAllLongData returns everything the connection holds, once.
func (c *Conn) releaseAllLongData() {
	c.longDataMu.Lock()
	if c.longDataReleased {
		c.longDataMu.Unlock()
		return
	}
	c.longDataReleased = true
	n := c.longDataHeld
	c.longDataHeld = 0
	c.longDataMu.Unlock()
	if h, ok := c.h.(LongDataHandler); ok && n > 0 {
		h.ReleaseLongData(n)
	}
}

// maxLongDataSize is the per-parameter long data bound.
func (c *Conn) maxLongDataSize() int {
	if c.serverConf == nil || c.serverConf.MaxLongDataSize() <= 0 {
		return DefaultMaxLongDataSize
	}
	return c.serverConf.MaxLongDataSize()
}

// errLongDataTooLong is MySQL's answer to long data past max_allowed_packet.
func errLongDataTooLong() error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR,
		"Parameter of prepared statement which is set through mysql_send_long_data() is longer than 'max_allowed_packet' bytes")
}

func (c *Conn) writePrepare(s *Stmt) error {
	data := make([]byte, 4, 128)

	// status ok
	data = append(data, 0)
	// stmt id
	data = append(data, mysql.Uint32ToBytes(s.ID)...)
	// number columns
	data = append(data, mysql.Uint16ToBytes(uint16(s.Columns))...)
	// number params
	data = append(data, mysql.Uint16ToBytes(uint16(s.Params))...)
	// filter [00]
	data = append(data, 0)
	// warning count
	data = append(data, 0, 0)

	if err := c.WritePacket(data); err != nil {
		return err
	}

	if s.Params > 0 {
		for i := 0; i < s.Params; i++ {
			data = data[0:4]
			if s.RawParamFields != nil && i < len(s.RawParamFields) {
				data = append(data, s.RawParamFields[i]...)
			} else {
				data = append(data, paramFieldData...)
			}

			if err := c.WritePacket(data); err != nil {
				return errors.Trace(err)
			}
		}

		// with CLIENT_DEPRECATE_EOF the parameter definitions have no
		// trailing EOF separator
		if !c.deprecateEOF() {
			if err := c.writeEOF(); err != nil {
				return err
			}
		}
	}

	if s.Columns > 0 {
		for i := 0; i < s.Columns; i++ {
			data = data[0:4]
			if s.RawColumnFields != nil && i < len(s.RawColumnFields) {
				data = append(data, s.RawColumnFields[i]...)
			} else {
				data = append(data, columnFieldData...)
			}

			if err := c.WritePacket(data); err != nil {
				return errors.Trace(err)
			}
		}

		// with CLIENT_DEPRECATE_EOF the column definitions have no trailing
		// EOF separator
		if !c.deprecateEOF() {
			if err := c.writeEOF(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Conn) handleStmtExecute(data []byte) (*mysql.Result, error) {
	if len(data) < 9 {
		return nil, errMalformedPacket()
	}

	pos := 0
	id := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	s, ok := c.stmts[id]
	if !ok {
		return nil, mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER, 5,
			strconv.FormatUint(uint64(id), 10), "stmt_execute")
	}

	// Long data and bound values are consumed by this execution whatever its
	// outcome, including a rejected flag, as in MySQL; the bound types
	// persist.
	defer c.resetStmtParams(s)

	if s.longDataErr != nil {
		return nil, s.longDataErr
	}

	flag := data[pos]
	pos++
	// Supported types:
	// - CURSOR_TYPE_NO_CURSOR
	// - PARAMETER_COUNT_AVAILABLE

	// Make sure the first 4 bits are 0.
	if flag>>4 != 0 {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flags 0x%x", flag))
	}

	// Test for unsupported flags in the remaining 4 bits.
	if flag&mysql.CURSOR_TYPE_READ_ONLY > 0 {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "unsupported flag CURSOR_TYPE_READ_ONLY")
	}
	if flag&mysql.CURSOR_TYPE_FOR_UPDATE > 0 {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "unsupported flag CURSOR_TYPE_FOR_UPDATE")
	}
	if flag&mysql.CURSOR_TYPE_SCROLLABLE > 0 {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "unsupported flag CURSOR_TYPE_SCROLLABLE")
	}

	// skip iteration-count, always 1
	pos += 4

	paramNum := s.Params

	if paramNum > 0 {
		nullBitmapLen := (s.Params + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return nil, errMalformedPacket()
		}
		nullBitmaps := data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		// new-params-bound flag: when 1 the parameter types follow; when 0
		// the client reuses the types it bound last. Parameter values follow
		// either way.
		newParamsBound := data[pos]
		pos++
		if newParamsBound == 1 {
			if len(data) < (pos + (paramNum << 1)) {
				return nil, errMalformedPacket()
			}
			// Copied defensively: the types outlive this packet, which belongs
			// to the caller.
			s.paramTypes = append(s.paramTypes[:0], data[pos:pos+(paramNum<<1)]...)
			pos += paramNum << 1
		}

		if err := c.bindStmtArgs(s, nullBitmaps, s.paramTypes, data[pos:]); err != nil {
			return nil, errors.Trace(err)
		}
	}

	var r *mysql.Result
	var err error
	if r, err = c.h.HandleStmtExecute(s.Context, s.Query, s.Args); err != nil {
		return nil, errors.Trace(err)
	}

	return r, nil
}

func (c *Conn) bindStmtArgs(s *Stmt, nullBitmap, paramTypes, paramValues []byte) error {
	args := s.Args

	// Every param should have a type-and-flag of 2 bytes
	// 0xfe80 == Type 0xfe and Flag 0x80
	// The flag only has one bit and that indicates if it is unsigned or not.
	// Types are 1 byte, but might grow into the 7 unused bits in the future.
	// paramTypes is nil when the client has never bound types; that is only
	// valid while every parameter is NULL, as in MySQL.
	if paramTypes != nil && len(paramTypes)/2 != s.Params {
		return errMalformedPacket()
	}

	pos := 0

	var v []byte
	var n int
	var isNull bool
	var err error

	for i := 0; i < s.Params; i++ {
		// A parameter whose value arrived through COM_STMT_SEND_LONG_DATA has
		// no value in this packet, and its NULL bit is ignored, as in MySQL.
		if i < len(s.longData) && s.longData[i] != nil {
			// MySQL: ER_MALFORMED_PACKET when the value's type is unknowable,
			// or is not a string or blob type.
			if paramTypes == nil || !isLongDataType(paramTypes[i<<1]) {
				return errMalformedPacket()
			}
			args[i] = mysql.TypedBytes{Type: paramTypes[i<<1], Bytes: s.longData[i]}
			continue
		}

		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		if paramTypes == nil {
			// MySQL: ER_WRONG_ARGUMENTS, a value with no type to decode it by.
			return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "mysqld_stmt_execute")
		}
		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & mysql.PARAM_UNSIGNED) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return errMalformedPacket()
			}

			if isUnsigned {
				args[i] = paramValues[pos]
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return errMalformedPacket()
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint16(paramValues[pos : pos+2])
			} else {
				args[i] = int16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return errMalformedPacket()
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint32(paramValues[pos : pos+4])
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return errMalformedPacket()
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return errMalformedPacket()
			}

			args[i] = math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return errMalformedPacket()
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_BIT,
			mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB,
			mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING,
			mysql.MYSQL_TYPE_GEOMETRY, mysql.MYSQL_TYPE_VECTOR,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return errMalformedPacket()
			}

			v, isNull, n, err = mysql.LengthEncodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return errMalformedPacket()
			}

			if !isNull {
				args[i] = mysql.TypedBytes{Type: tp, Bytes: v}
				continue
			}
			args[i] = nil
			continue
		default:
			// MySQL: ER_WRONG_ARGUMENTS for a type it does not know.
			return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "mysqld_stmt_execute")
		}
	}
	return nil
}

// errMalformedPacket is MySQL's ER_MALFORMED_PACKET (1835).
// mysql.ErrMalformPacket is a plain error and would reach the client as 1105.
func errMalformedPacket() error {
	return mysql.NewDefaultError(mysql.ER_MALFORMED_PACKET)
}

// isLongDataType reports whether MySQL accepts COM_STMT_SEND_LONG_DATA for a
// parameter of type tp: only the string and blob types.
func isLongDataType(tp byte) bool {
	switch tp {
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB,
		mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING:
		return true
	}
	return false
}

// stmt send long data command has no response
func (c *Conn) handleStmtSendLongData(data []byte) error {
	if len(data) < 6 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return nil
	}

	paramID := binary.LittleEndian.Uint16(data[4:6])
	if paramID >= uint16(s.Params) {
		return nil
	}

	// A statement in the error state ignores long data until
	// COM_STMT_RESET.
	if s.longDataErr != nil {
		return nil
	}

	if s.longData == nil {
		s.longData = make([][]byte, s.Params)
	}
	chunk := data[6:]
	if len(s.longData[paramID])+len(chunk) > c.maxLongDataSize() {
		c.failLongData(s, errLongDataTooLong())
		return nil
	}
	if err := c.reserveLongData(len(chunk)); err != nil {
		c.failLongData(s, err)
		return nil
	}
	// append copies defensively: the value outlives this packet, which
	// belongs to the caller. A non-nil empty slice still marks the parameter
	// as sent through long data.
	if s.longData[paramID] == nil {
		s.longData[paramID] = make([]byte, 0, len(chunk))
	}
	s.longData[paramID] = append(s.longData[paramID], chunk...)
	s.longDataSize += len(chunk)

	return nil
}

// failLongData puts s in the error state with err, discarding its long data.
func (c *Conn) failLongData(s *Stmt, err error) {
	c.resetStmtParams(s)
	s.longDataErr = err
}

func (c *Conn) handleStmtReset(data []byte) (*mysql.Result, error) {
	if len(data) < 4 {
		return nil, errMalformedPacket()
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return nil, mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER, 5,
			strconv.FormatUint(uint64(id), 10), "stmt_reset")
	}

	c.resetStmtParams(s)
	s.longDataErr = nil

	return mysql.NewResultReserveResultset(0), nil
}

// stmt close command has no response
func (c *Conn) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	stmt, ok := c.stmts[id]
	if !ok {
		return nil
	}

	// The client has no reply to wait for and will not use the statement
	// again, so it is deallocated whatever the handler returns, as
	// ResetStmts does.
	err := c.h.HandleStmtClose(stmt.Context)
	c.resetStmtParams(stmt)
	delete(c.stmts, id)

	return err
}

// ResetStmts deallocates every prepared statement on the connection, calling
// HandleStmtClose for each, as MySQL does on COM_RESET_CONNECTION and
// COM_CHANGE_USER. A handler that implements COM_RESET_CONNECTION through
// HandleOtherCommand calls it so that statement IDs the client no longer
// holds stop resolving. Every statement is removed even if HandleStmtClose
// fails; the errors are joined.
func (c *Conn) ResetStmts() error {
	var errs []error
	for id, st := range c.stmts {
		if err := c.h.HandleStmtClose(st.Context); err != nil {
			errs = append(errs, err)
		}
		c.resetStmtParams(st)
		delete(c.stmts, id)
	}
	return stderrors.Join(errs...)
}
