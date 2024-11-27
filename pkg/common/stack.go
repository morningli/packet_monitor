package common

import "container/list"

type Stack struct {
	v *list.List
}

func NewStack() *Stack {
	return &Stack{v: list.New()}
}

func (s *Stack) Push(v interface{}) {
	s.v.PushFront(v)
}

func (s *Stack) Pop() interface{} {
	e := s.v.Front()
	if e == nil {
		return nil
	}
	s.v.Remove(e)
	return e.Value
}

func (s *Stack) Size() int {
	return s.v.Len()
}
