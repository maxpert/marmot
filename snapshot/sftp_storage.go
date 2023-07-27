package snapshot

import (
	"net"
	"net/url"
	"os"
	"path"

	"github.com/maxpert/marmot/cfg"
	"github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

type sftpStorage struct {
	client     *sftp.Client
	uploadPath string
}

func (s *sftpStorage) Upload(name, filePath string) error {
	err := s.client.MkdirAll(s.uploadPath)
	if err != nil {
		return err
	}

	srcFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	uploadPath := path.Join(s.uploadPath, name)
	dstFile, err := s.client.OpenFile(uploadPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	bytes, err := dstFile.ReadFrom(srcFile)
	if err != nil {
		return err
	}

	log.Info().
		Str("file_name", name).
		Str("file_path", filePath).
		Str("sftp_path", uploadPath).
		Int64("bytes", bytes).
		Msg("Snapshot uploaded to SFTP server")
	return nil
}

func (s *sftpStorage) Download(filePath, name string) error {
	remotePath := path.Join(s.uploadPath, name)
	srcFile, err := s.client.Open(remotePath)
	if err != nil {
		if err.Error() == "file does not exist" {
			return ErrNoSnapshotFound
		}
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	bytes, err := dstFile.ReadFrom(srcFile)
	log.Info().
		Str("file_name", name).
		Str("file_path", filePath).
		Str("sftp_path", remotePath).
		Int64("bytes", bytes).
		Msg("Snapshot downloaded from SFTP server")
	return err
}

func newSFTPStorage() (*sftpStorage, error) {
	// Get the SFTP URL from the environment
	sftpURL := cfg.Config.Snapshot.SFTP.Url
	u, err := url.Parse(sftpURL)
	if err != nil {
		return nil, err
	}

	password, hasPassword := u.User.Password()
	authMethod := make([]ssh.AuthMethod, 0)
	if hasPassword {
		authMethod = append(authMethod, ssh.Password(password))
	}

	// Set up the SSH config
	config := &ssh.ClientConfig{
		User: u.User.Username(),
		Auth: authMethod,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			log.Info().
				Str("hostname", hostname).
				Str("remote", remote.String()).
				Str("public_key_type", key.Type()).
				Msg("Host connected for SFTP storage")
			return nil
		},
		BannerCallback: func(message string) error {
			log.Info().Str("message", message).Msgf("Server message...")
			return nil
		},
	}

	// Connect to the SSH server
	conn, err := ssh.Dial("tcp", u.Host, config)
	if err != nil {
		return nil, err
	}

	// Open the SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &sftpStorage{client: client, uploadPath: u.Path}, nil
}
