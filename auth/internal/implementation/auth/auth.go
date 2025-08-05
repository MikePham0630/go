package auth

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"

	pb "github.com/MikePham0630/gomicro/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jwt "github.com/golang-jwt/jwt/v5"
)

type Implementation struct {
	db *sql.DB
	pb.UnimplementedAuthServiceServer
}

func NewAuthImplementation(db *sql.DB) *Implementation {
	return &Implementation{
		db: db,
	}
}

func (this *Implementation) GetToken(ctx context.Context, credentials *pb.Credentials) (*pb.Token, error) {
	type User struct {
		userID   string
		password string
	}

	var u User

	stmt, err := this.db.Prepare("SELECT user_id, password FROM users WHERE user_id = ? AND password = ?")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to prepare statement: %v", err)
	}

	err = stmt.QueryRow(credentials.GetUsername(), credentials.GetPassword()).Scan(&u.userID, &u.password)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.Unauthenticated, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to query user: %v", err)
	}

	jwt, err := createJWMT(u.userID)
	return &pb.Token{
		Jwt: jwt,
	}, nil
}

func createJWMT(userID string) (string, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	now := time.Now()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "auth-service",
		"sub": userID,
		"exp": now.Add(24 * time.Hour).Unix(),
		"iat": now.Unix(),
	})

	signedToken, err := token.SignedString(key)
	if err != nil {
		log.Printf("Error signing token: %v", err)
		return "", status.Errorf(codes.Internal, "failed to sign token: %v", err)
	}

	return signedToken, nil
}

func (this *Implementation) ValidateToken(ctx context.Context, token *pb.Token) (*pb.User, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	userID, err := validateJWT(token.Jwt, key)
	if err != nil {
		return nil, err
	}
	return &pb.User{
		UserId: userID,
	}, nil
}

func validateJWT(tokenString string, signingKey []byte) (string, error) {

	type MyClaims struct {
		jwt.RegisteredClaims
	}

	parseToken, err := jwt.ParseWithClaims(tokenString, &MyClaims{}, func(token *jwt.Token) (interface{}, error) {
		return signingKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrSignatureInvalid) {
			return "", status.Errorf(codes.Unauthenticated, "invalid token signature")
		} else {
			return "", status.Errorf(codes.Internal, "failed to parse token: %v", err)
		}
	}

	claims, ok := parseToken.Claims.(*MyClaims)
	if !ok || !parseToken.Valid {
		return "", status.Errorf(codes.Unauthenticated, "invalid token claims")
	}

	return claims.RegisteredClaims.Subject, nil

}
