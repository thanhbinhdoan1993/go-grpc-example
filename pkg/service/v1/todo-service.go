package v1

import (
	"context"
	"database/sql"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/golang/protobuf/ptypes"
	v1 "github.com/thanhbinhdoan1993/go-grpc-example/pkg/api/v1"
)

const (
	// apiVersion is version of API is provided by server
	apiVersion = "v1"
)

// toDoServiceServer is implementation of v1.ToDoServiceServer proto interface
type toDoServiceServer struct {
	db *sql.DB
}

// NewToDoServiceServer creates ToDo service
func NewToDoServiceServer(db *sql.DB) v1.ToDoServiceServer {
	return &toDoServiceServer{
		db: db,
	}
}

// checkAPI checks if the API version requested by client is supported by server
func (s *toDoServiceServer) checkAPI(api string) error {
	// API version is "" means use current version of the service
	if len(api) > 0 {
		if apiVersion != api {
			return status.Errorf(codes.Unimplemented, "unsupported API version: service implements API version '%s' but asked for '%s'", apiVersion, api)
		}
	}
	return nil
}

// connect returns SQL database connection from the pool
func (s *toDoServiceServer) connect(ctx context.Context) (*sql.Conn, error) {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect to database, %+v", err)
	}
	return c, nil
}

// Create new todo task
func (s *toDoServiceServer) Create(ctx context.Context, req *v1.CreateRequest) (*v1.CreateResponse, error) {
	// Check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(req.ToDo.Reminder)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reminder field has invalid format, %+v", err)
	}

	// insert ToDo entiry data
	res, err := c.ExecContext(ctx, "INSERT INTO ToDo(`Title`, `Description`, `Reminder`) VALUES(?, ?, ?)",
		req.ToDo.GetTitle(), req.ToDo.GetDescription(), reminder)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert into ToDo, %+v", err)
	}

	// get ID of creates ToDo
	id, err := res.LastInsertId()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrive id for created ToDo, %+v", err)
	}

	return &v1.CreateResponse{Api: apiVersion, Id: id}, nil
}

// Read todo task
func (s *toDoServiceServer) Read(ctx context.Context, req *v1.ReadRequest) (*v1.ReadResponse, error) {
	// check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// request todo by ID
	rows, err := s.db.QueryContext(ctx, "SELECT `ID`, `Title`, `Description`, `Reminder` FROM ToDo WHERE `ID`=?", req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to select from ToDo, %+v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to retrieve data from ToDo, %+v", err)
		}
		return nil, status.Errorf(codes.NotFound, "ToDo with ID='%d' is not found", req.Id)
	}

	// get ToDo data
	var td v1.ToDo
	var reminder time.Time

	if err := rows.Scan(&td.Id, &td.Title, &td.Description, &reminder); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve field value from ToDo row, %+v", err)
	}
	td.Reminder, err = ptypes.TimestampProto(reminder)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reminder field has invalid format, %+v", err)
	}

	if rows.Next() {
		return nil, status.Errorf(codes.Internal, "found multiple ToDo rows with ID='%d'", req.Id)
	}

	return &v1.ReadResponse{Api: apiVersion, ToDo: &td}, nil
}

// Update todo task
func (s *toDoServiceServer) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.UpdateResponse, error) {
	// check if the API version request by client is support by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	reminder, err := ptypes.Timestamp(req.ToDo.GetReminder())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reminder filed has invalid format, %+v", err)
	}

	// update ToDo
	res, err := c.ExecContext(ctx, "UPDATE ToDo SET `Title`=?, `Description`=?, `Reminder`=? WHERE `ID`=?",
		req.ToDo.GetTitle(), req.ToDo.GetDescription(), reminder, req.ToDo.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update ToDo, %+v", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve rows affected, %+v", err)
	}

	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "ToDo with ID='%d' is not found", req.ToDo.GetId())
	}

	return &v1.UpdateResponse{Api: apiVersion, Updated: rows}, nil
}

// Delete todo task
func (s *toDoServiceServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	// check if the API version requested by client is support by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// delete ToDo
	res, err := c.ExecContext(ctx, "DELETE FROM ToDo WHERE `ID`=?", req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete ToDo, %+v", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve rows affected, %+v", err)
	}

	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "ToDo with ID='%d' is not found", req.Id)
	}

	return &v1.DeleteResponse{Api: apiVersion, Deleted: rows}, nil
}

// Read all todo taks
func (s *toDoServiceServer) ReadAll(ctx context.Context, req *v1.ReadAllRequest) (*v1.ReadAllResponse, error) {
	// check if the API version requested by client is supported by server
	if err := s.checkAPI(req.Api); err != nil {
		return nil, err
	}

	// get SQL connection from pool
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// get ToDo list
	rows, err := c.QueryContext(ctx, "SELECT `ID`, `Title`, `Description`, `Reminder` FROM ToDo")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to select from ToDo, %+v", err)
	}
	defer rows.Close()

	var reminder time.Time
	list := []*v1.ToDo{}

	for rows.Next() {
		td := new(v1.ToDo)
		if err := rows.Scan(td.Id, td.Title, td.Description, &reminder); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to retrieve field value from rows, %+v", err)
		}

		td.Reminder, err = ptypes.TimestampProto(reminder)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "reminder field has invalid format, %+v", err)
		}
		list = append(list, td)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve data from ToDo rows, %+v", err)
	}

	return &v1.ReadAllResponse{Api: apiVersion, ToDos: list}, nil
}
