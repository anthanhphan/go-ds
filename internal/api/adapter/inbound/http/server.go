package http_handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	sdklogger "github.com/anthanhphan/gosdk/logger"
	"github.com/gofiber/fiber/v2"
	fiberlogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

type Server struct {
	app     *fiber.App
	cfg     *config.Config
	service port.FileService
}

func NewServer(cfg *config.Config, service port.FileService) *Server {
	app := fiber.New(fiber.Config{
		BodyLimit:         int(cfg.App.MaxFileSize), // Use MaxFileSize from config
		StreamRequestBody: true,
	})

	// Middleware
	app.Use(recover.New())
	app.Use(fiberlogger.New())

	s := &Server{
		app:     app,
		cfg:     cfg,
		service: service,
	}

	// Routes
	s.registerRoutes()

	return s
}

func (s *Server) registerRoutes() {
	s.app.Post("/files", s.handleUpload)
	s.app.Get("/files", s.handleDownload)
	s.app.Get("/files/metadata", s.handleMetadata)
}

func (s *Server) Start() error {
	return s.app.Listen(s.cfg.Server.Addr)
}

func (s *Server) Stop(ctx context.Context) error {
	return s.app.Shutdown()
}

func (s *Server) sendJSONError(c *fiber.Ctx, status int, message string) error {
	return c.Status(status).JSON(fiber.Map{
		"error": message,
	})
}

func (s *Server) handleMetadata(c *fiber.Ctx) error {
	fileID := c.Query("id")
	if fileID == "" {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Missing 'id' query parameter")
	}

	meta, err := s.service.GetFileMetadata(c.Context(), fileID)
	if err != nil {
		sdklogger.Warnw("Metadata lookup failed", "file_id", fileID, "error", err.Error())
		return s.sendJSONError(c, fiber.StatusNotFound, fmt.Sprintf("Metadata not found: %v", err))
	}

	return c.JSON(meta)
}

func (s *Server) handleUpload(c *fiber.Ctx) error {
	contentType := c.Get("Content-Type")
	if !strings.HasPrefix(contentType, "multipart/form-data") {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Content-Type must be multipart/form-data")
	}

	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Invalid Content-Type")
	}
	boundary, ok := params["boundary"]
	if !ok {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Missing boundary in Content-Type")
	}

	// Use raw request body stream
	bodyStream := c.Context().RequestBodyStream()
	if bodyStream == nil {
		bodyStream = bytes.NewReader(c.Body())
	}
	mr := multipart.NewReader(bodyStream, boundary)

	var fileName string
	var src io.Reader

	// Find the file part
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return s.sendJSONError(c, fiber.StatusInternalServerError, fmt.Sprintf("Failed to read multipart: %v", err))
		}

		if part.FileName() != "" {
			fileName = part.FileName()
			src = part
			break
		}
		// If not file, we can potentially read other fields here
		_ = part.Close()
	}

	if src == nil {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Missing 'file' part")
	}

	finalFileID, err := s.service.UploadFile(c.Context(), fileName, src)
	if err != nil {
		sdklogger.Errorw("Upload failed", "file_name", fileName, "error", err.Error())
		return s.sendJSONError(c, fiber.StatusInternalServerError, fmt.Sprintf("Upload failed: %v", err))
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "File uploaded successfully (true streaming)",
		"id":      finalFileID,
	})
}

func (s *Server) handleDownload(c *fiber.Ctx) error {
	fileID := c.Query("id")
	if fileID == "" {
		return s.sendJSONError(c, fiber.StatusBadRequest, "Missing 'id' query parameter")
	}

	// Get metadata to find original filename
	meta, err := s.service.GetFileMetadata(c.Context(), fileID)
	outFileName := fileID
	if err == nil && meta.FileName != "" {
		outFileName = meta.FileName
	}

	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", outFileName))
	c.Set("Content-Type", "application/octet-stream")

	if err := s.service.DownloadFile(c.Context(), fileID, c.Response().BodyWriter()); err != nil {
		// If we haven't written anything yet, we can change status.
		// But DownloadFile writes chunks.
		// We'll log error.
		sdklogger.Errorw("Download failed", "file_id", fileID, "error", err.Error())
		return fmt.Errorf("download failed: %w", err)
	}

	return nil
}
