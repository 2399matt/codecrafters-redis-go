package main

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sync"
)

type User struct {
	username  string
	nopass    bool
	flags     []string
	passwords []string
}

type ACL struct {
	mu    *sync.Mutex
	users map[string]*User
}

func (a *ACL) Get(username string) *User {
	a.mu.Lock()
	defer a.mu.Unlock()
	u := a.users[username]
	return u
}

func (a *ACL) createDefaultUser() {
	a.mu.Lock()
	defer a.mu.Unlock()
	user := &User{username: "default", flags: []string{"nopass"}}
	a.users[user.username] = user
}

// check to see if we actually need the lock here. We have a pointer in admin to modify.
func (a *ACL) updateUser(user *User, password string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	hash := sha256.Sum256([]byte(password))
	user.passwords = append(user.passwords, hex.EncodeToString(hash[0:]))
	user.nopass = false
	user.flags = slices.DeleteFunc(user.flags, func(s string) bool { return s == "nopass" })
}
