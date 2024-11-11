package main

import (
	"context"
	"fmt"
	pb "github.com/alexeyvas94/hw_grpc/pkg"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"net"
	"sync"
	"time"
)

func parseRole(roleStr string) (pb.Role, error) {
	switch roleStr {
	case "ADMIN":
		return pb.Role_ADMIN, nil
	case "USER":
		return pb.Role_USER, nil
	default:
		return pb.Role_USER, fmt.Errorf("invalid role: %s", roleStr)
	}
}
func roleToString(role pb.Role) string {
	switch role {
	case pb.Role_ADMIN:
		return "ADMIN"
	case pb.Role_USER:
		return "USER"
	default:
		return "USER" // Значение по умолчанию
	}
}

const (
	dbDSN = "host=localhost port=54321 dbname=note user=note-user password=note-password sslmode=disable"
)

type server struct {
	pb.UnimplementedUserServer
	mu sync.RWMutex
}

// Метод добавления нового пользователя в БД
func (s *server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Преобразуем значение role в строку
	role := roleToString(req.Role)

	// Создаем соединение с базой данных
	con, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer con.Close(ctx)

	// Переменная для хранения id новой записи
	var userID int64

	// Делаем запрос на вставку записи в таблицу users и возвращаем id новой записи
	err = con.QueryRow(ctx,
		"INSERT INTO users (name, email, role, password) VALUES ($1, $2, $3, $4) RETURNING id",
		req.Name, req.Email, role, req.Password).Scan(&userID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert user: %v", err)
	}
	log.Printf("Inserted user with id %d", userID)

	// Возвращаем ответ с id добавленного пользователя
	return &pb.CreateResponse{Id: userID}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Создаем соединение с базой данных
	con, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer con.Close(ctx)

	// Переменные для хранения данных пользователя
	var userID int64
	var name, email, role, password string
	var created_at, update_at time.Time

	// Выполняем запрос для получения данных пользователя по его id
	err = con.QueryRow(ctx, "SELECT id, name, email, role, password, created_at, updated_at FROM users WHERE id = $1",
		req.Id).Scan(&userID, &name, &email, &role, &password, &created_at, &update_at)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Если пользователь с указанным id не найден
			return nil, fmt.Errorf("user with id %d not found", req.Id)
		}
		return nil, fmt.Errorf("failed to get user: %v", err)
	}
	// Преобразуем строку роли в тип Role
	rolestr, err := parseRole(role)
	if err != nil {
		return nil, err
	}

	// Формируем и возвращаем ответ с данными пользователя
	return &pb.GetResponse{
		User: &pb.Is_User{
			Id:              userID,
			Name:            name,
			Email:           email,
			Role:            rolestr,
			Password:        password,
			PasswordConfirm: password,
			CreatedAt:       timestamppb.New(created_at),
			UpdateAt:        timestamppb.New(update_at),
		},
	}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Создаем соединение с базой данных
	con, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer con.Close(ctx)

	// Выполняем запрос на удаление пользователя по id
	res, err := con.Exec(ctx, "DELETE FROM users WHERE id = $1", req.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to delete user: %v", err)
	}

	// Проверяем, что удалено хотя бы одно совпадение
	if res.RowsAffected() == 0 {
		return nil, fmt.Errorf("user with id %d not found", req.Id)
	}

	// Возвращаем пустой ответ при успешном удалении
	return &emptypb.Empty{}, nil
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Преобразуем значение role в строку
	role := roleToString(req.Role)
	// Создаем соединение с базой данных
	con, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer con.Close(ctx)

	// Выполняем запрос на обновление данных пользователя по id
	res, err := con.Exec(ctx,
		"UPDATE users SET name = $1, email = $2, role = $3 WHERE id = $4",
		req.Name.Value, req.Email.Value, role, req.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to update user: %v", err)
	}

	// Проверяем, что обновлена хотя бы одна строка
	if res.RowsAffected() == 0 {
		return nil, fmt.Errorf("user with id %d not found", req.Id)
	}

	// Возвращаем пустой ответ при успешном обновлении
	return &emptypb.Empty{}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Серверу пизда: %v", err)
	}
	s := grpc.NewServer()
	reflection.Register(s)
	pb.RegisterUserServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Ошибка при запуске сервера: %v", err)
	}

}
