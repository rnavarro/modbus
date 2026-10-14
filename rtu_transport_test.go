package modbus

import (
	"bytes"
	"errors"
	"testing"
	"io"
	"net"
	"time"
)

func TestAssembleRTUFrame(t *testing.T) {
	var rt		*rtuTransport
	var frame	[]byte

	rt = &rtuTransport{}

	frame = rt.assembleRTUFrame(&pdu{
		unitId:       0x33,
		functionCode: 0x11,
		payload:      []byte{0x22, 0x33, 0x44, 0x55},
	})
	// expect 1 byte of unit id, 1 byte of function code, 4 bytes of payload and
	// 2 bytes of CRC
	if len(frame) != 8 {
		t.Errorf("expected 8 bytes, got %v", len(frame))
	}
	for i, b := range []byte{
		0x33, 0x11, // unit id and function code
		0x22, 0x33, // payload
		0x44, 0x55, // payload
		0xf0, 0x93, // CRC
	} {
		if frame[i] != b {
			t.Errorf("expected 0x%02x at position %v, got 0x%02x", b, i, frame[i])
		}
	}

	frame = rt.assembleRTUFrame(&pdu{
		unitId:       0x31,
		functionCode: 0x06,
		payload:      []byte{0x12, 0x34},
	})
	// expect 1 byte of unit if, 1 byte of function code, 2 bytes of payload and
	// 2 bytes of CRC
	if len(frame) != 6 {
		t.Errorf("expected 6 bytes, got %v", len(frame))
	}
	for i, b := range []byte{
		0x31, 0x06, // unit id and function code
		0x12, 0x34, // payload
		0xe3, 0xae, // CRC
	} {
		if frame[i] != b {
			t.Errorf("expected 0x%02x at position %v, got 0x%02x", b, i, frame[i])
		}
	}

	return
}


func TestRTUTransportReadRTUFrame(t *testing.T) {
	var rt		*rtuTransport
	var p1, p2	net.Conn
	var txchan	chan []byte
	var err		error
	var res		*pdu

	txchan = make(chan []byte, 2)
	p1, p2 = net.Pipe()
	go feedTestPipe(t, txchan, p1)

	// feed garbage to the transport on init to simulate
	// data buffering by the RX path of the serial driver while the
	// device is closed: the transport should discard it and
	// proceed to normal operation
	txchan <- []byte{0xfa, 0x8d, 0xcc, 0x1b, 0xf9,}

	rt = newRTUTransport(p2, "", 9600, 10 * time.Millisecond, nil)

	// arm a read/write timeout to avoid deadlocked tests
	p2.SetDeadline(time.Now().Add(100*time.Millisecond))

	// read a valid response (illegal data address)
	txchan <- []byte{
		0x31, 0x82, // unit id and response code
		0x02,       // exception code
		0xc1, 0x6e, // CRC
	}
	res, err = rt.readRTUFrame()

	if err != nil {
		t.Errorf("readRTUFrame() should have succeeded, got %v", err)
	}
	if res.unitId != 0x31 {
		t.Errorf("expected 0x31 as unit id, got 0x%02x", res.unitId)
	}
	if res.functionCode != 0x82 {
		t.Errorf("expected 0x82 as function code, got 0x%02x", res.functionCode)
	}
	if len(res.payload) != 1 {
		t.Errorf("expected a length of 1, got %v", len(res.payload))
	}
	if res.payload[0] != 0x02 {
		t.Errorf("expected {0x02} as payload, got {0x%02x}",
			 res.payload[0])
	}

	// read a frame with a bad crc
	txchan <- []byte{
		0x30, 0x82, // unit id and response code
		0x12,       // exception code
		0xc0, 0xa2, // CRC
	}
	res, err = rt.readRTUFrame()
	if !errors.Is(err, ErrBadCRC) {
		t.Errorf("readRTUFrame() should have returned ErrBadCRC, got %v", err)
	}
	var fe *FrameError
	if !errors.As(err, &fe) {
		t.Errorf("expected a *FrameError carrying the frame, got %T", err)
	} else if !bytes.Equal(fe.Frame, []byte{0x30, 0x82, 0x12, 0xc0, 0xa2}) {
		t.Errorf("expected frame {30 82 12 c0 a2}, got % x", fe.Frame)
	}

	// read a longer, valid response
	txchan <- []byte{
		0x31, 0x03, // unit id and response code
		0x04,       // length
		0x11, 0x22, // register #1
		0x33, 0x44, // register #2
		0x7b, 0xc5, // CRC
	}
	res, err = rt.readRTUFrame()
	if err != nil {
		t.Errorf("readRTUFrame() should have succeeded, got %v", err)
	}
	if res.unitId != 0x31 {
		t.Errorf("expected 0x31 as unit id, got 0x%02x", res.unitId)
	}
	if res.functionCode != 0x03 {
		t.Errorf("expected 0x03 as function code, got 0x%02x", res.functionCode)
	}
	if len(res.payload) != 5 {
		t.Errorf("expected a length of 5, got %v", len(res.payload))
	}
	for i, b := range []byte{
		0x04,
		0x11, 0x22,
		0x33, 0x44,
	} {
		if res.payload[i] != b {
			t.Errorf("expected 0x%02x at position %v, got 0x%02x",
				 b, i, res.payload[i])
		}
	}

	p1.Close()
	p2.Close()

	return
}

func TestRTUTransportBadCRCErrorCarriesFrame(t *testing.T) {
	var rt		*rtuTransport
	var p1, p2	net.Conn
	var txchan	chan []byte
	var err		error
	var fe		*FrameError

	txchan = make(chan []byte, 1)
	p1, p2 = net.Pipe()
	go feedTestPipe(t, txchan, p1)

	rt = newRTUTransport(p2, "", 9600, 100 * time.Millisecond, nil)
	p2.SetDeadline(time.Now().Add(200 * time.Millisecond))

	// non-standard 8-byte reply some devices use to reject an unsupported
	// request: unit id, function code with the exception bit set, an echo of
	// the request bytes 2-5 and a CRC. There is no exception code byte, so the
	// library expects a 1-byte exception payload, reads the frame short and
	// computes the CRC over the wrong span.
	frame := withCRC([]byte{
		0x01, 0x83,             // unit id and function code | 0x80
		0x00, 0x00, 0x00, 0x01, // echo of the request bytes 2-5
	})

	txchan <- frame

	_, err = rt.readRTUFrame()
	if !errors.Is(err, ErrBadCRC) {
		t.Fatalf("expected ErrBadCRC, got %v", err)
	}
	if !errors.As(err, &fe) {
		t.Fatalf("expected a *FrameError carrying the frame, got %T", err)
	}
	// the library only read the 5 bytes it believed made up the frame
	if !bytes.Equal(fe.Frame, frame[:5]) {
		t.Errorf("expected frame % x, got % x", frame[:5], fe.Frame)
	}

	// drain the bytes the library did not read so the pipe writer can return
	// before the test closes the pipe
	io.ReadFull(p2, make([]byte, len(frame) - 5))

	p1.Close()
	p2.Close()

	return
}

func TestRTUTransportShortFrameErrorCarriesFrame(t *testing.T) {
	var rt		*rtuTransport
	var p1, p2	net.Conn
	var txchan	chan []byte
	var err		error
	var fe		*FrameError

	txchan = make(chan []byte, 1)
	p1, p2 = net.Pipe()
	go feedTestPipe(t, txchan, p1)

	rt = newRTUTransport(p2, "", 9600, 100 * time.Millisecond, nil)
	p2.SetDeadline(time.Now().Add(200 * time.Millisecond))

	// an incomplete header: the unit id and function code arrive, then the
	// line goes quiet
	txchan <- []byte{0x01, 0x83}

	_, err = rt.readRTUFrame()
	if !errors.Is(err, ErrShortFrame) {
		t.Fatalf("expected ErrShortFrame, got %v", err)
	}
	if !errors.As(err, &fe) {
		t.Fatalf("expected a *FrameError carrying the frame, got %T", err)
	}
	if !bytes.Equal(fe.Frame, []byte{0x01, 0x83}) {
		t.Errorf("expected frame {01 83}, got % x", fe.Frame)
	}

	p1.Close()
	p2.Close()

	return
}

func TestRTUTransportValidFrameHasNoFrameError(t *testing.T) {
	var rt		*rtuTransport
	var p1, p2	net.Conn
	var txchan	chan []byte
	var err		error
	var res		*pdu

	txchan = make(chan []byte, 1)
	p1, p2 = net.Pipe()
	go feedTestPipe(t, txchan, p1)

	rt = newRTUTransport(p2, "", 9600, 100 * time.Millisecond, nil)
	p2.SetDeadline(time.Now().Add(200 * time.Millisecond))

	txchan <- withCRC([]byte{0x01, 0x03, 0x02, 0x12, 0x34})

	res, err = rt.readRTUFrame()
	if err != nil {
		t.Fatalf("readRTUFrame() should have succeeded, got %v", err)
	}
	if res.unitId != 0x01 || res.functionCode != 0x03 {
		t.Errorf("unexpected response: unit 0x%02x, function 0x%02x",
			 res.unitId, res.functionCode)
	}
	if errors.Is(err, ErrBadCRC) {
		t.Errorf("a valid frame must not match ErrBadCRC")
	}

	p1.Close()
	p2.Close()

	return
}

// withCRC returns data with its modbus CRC appended.
func withCRC(data []byte) (frame []byte) {
	var c crc

	frame = append(frame, data...)
	c.init()
	c.add(data)
	frame = append(frame, c.value()...)

	return
}

func feedTestPipe(t *testing.T, in chan []byte, out io.WriteCloser) {
	var err   error
	var txbuf []byte

	for {
		// grab a slice of bytes from the channel
		txbuf = <-in

		// write this slice to the pipe
		_, err = out.Write(txbuf)
		if err != nil {
			t.Errorf("failed to write to test pipe: %v", err)
			return
		}
	}

	return
}

func TestModbusRTUSerialCharTime(t *testing.T) {
	var d time.Duration

	d = serialCharTime(38400)
	// expect 11 bits at 38400bps: 11 * (1/38400) = 286.458uS
	if d != time.Duration(286458) * time.Nanosecond {
		t.Errorf("unexpected serial char duration: %v", d)
	}

	d = serialCharTime(19200)
	// expect 11 bits at 19200bps: 11 * (1/19200) = 572.916uS
	if d != time.Duration(572916) * time.Nanosecond {
		t.Errorf("unexpected serial char duration: %v", d)
	}

	d = serialCharTime(9600)
	// expect 11 bits at 9600bps: 11 * (1/9600) = 1.145833ms
	if d != time.Duration(1145833) * time.Nanosecond {
		t.Errorf("unexpected serial char duration: %v", d)
	}

	return
}
