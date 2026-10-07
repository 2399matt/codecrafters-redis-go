package main

import "sync"

type User struct {
	username  string
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
