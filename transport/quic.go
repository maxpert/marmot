package transport

import (
    "context"
    "crypto/tls"
    "fmt"
    "time"

    "github.com/fxamacker/cbor/v2"
    "github.com/google/uuid"
    "github.com/lucas-clemente/quic-go"
    "github.com/rs/zerolog/log"
)

var machineID = uuid.New()
var serverCert, clientCert *tls.Config
var quicConfig = &quic.Config{
    EnableDatagrams: true,
    MaxIdleTimeout:  10 * time.Minute,
    KeepAlivePeriod: 5 * time.Second,
}

type QUICTransport struct {
    listener quic.Listener
    id       uuid.UUID
}

type QUICSession struct {
    conn    quic.Connection
    stream  quic.Stream
    id      uuid.UUID
    decoder *cbor.Decoder
}

type idPayload struct {
    From    [16]byte `cbor:"f"`
    Payload any      `cbor:"p"`
}

func init() {
    cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
    if err != nil {
        panic(err)
    }

    serverCert = &tls.Config{
        Certificates: []tls.Certificate{cer},
        NextProtos:   []string{"quic"},
    }
    clientCert = &tls.Config{
        InsecureSkipVerify: true,
        NextProtos:         []string{"quic"},
    }
}

func NewQUICClient(addr string, ctx context.Context) (*QUICSession, error) {
    conn, err := quic.DialAddr(addr, clientCert, quicConfig)
    if err != nil {
        return nil, err
    }

    stream, err := conn.OpenStreamSync(ctx)
    if err != nil {
        return nil, err
    }

    return &QUICSession{
        conn:    conn,
        stream:  stream,
        id:      machineID,
        decoder: cbor.NewDecoder(stream),
    }, nil
}

func NewQUICServer(addr string) (*QUICTransport, error) {
    listener, err := quic.ListenAddr(addr, serverCert, quicConfig)
    if err != nil {
        return nil, err
    }

    return &QUICTransport{
        listener: listener,
        id:       machineID,
    }, nil
}

func (q *QUICTransport) Accept(ctx context.Context) (*QUICSession, error) {
    conn, err := q.listener.Accept(ctx)
    if err != nil {
        return nil, err
    }

    stream, err := conn.AcceptStream(ctx)
    if err != nil {
        return nil, err
    }

    log.Info().Msg(fmt.Sprintf("Peers connected %v <> %v", conn.LocalAddr(), conn.RemoteAddr()))
    return &QUICSession{
        conn:    conn,
        stream:  stream,
        decoder: cbor.NewDecoder(stream),
        id:      q.id,
    }, nil
}

func (q *QUICTransport) Close() {
    err := q.listener.Close()
    if err != nil {
        log.Error().Err(err).Msg("Unable to close QUICTransport")
    }
}

func (s *QUICSession) Read() (any, error) {
    for {
        d := idPayload{}
        if err := s.decoder.Decode(&d); err != nil {
            return nil, err
        }

        if d.From == s.id || d.Payload == nil {
            log.Warn().Msg("Dropping loopback/nil message")
            continue
        }

        return d.Payload, nil
    }
}

func (s *QUICSession) Write(m any) error {
    enc := cbor.NewEncoder(s.stream)
    err := enc.Encode(&idPayload{
        From:    s.id,
        Payload: m,
    })
    if err != nil {
        return err
    }

    return nil
}

func (s *QUICSession) Close() {
    err := s.stream.Close()
    if err != nil {
        log.Error().Err(err).Msg("Unable to close QUICSession")
    }
}

func (s *QUICSession) String() string {
    return s.conn.RemoteAddr().String()
}
