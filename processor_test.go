// Copyright 2023 Lowpower. All rights reserved.

package redisq

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var taskCmpOpts = []cmp.Option{
	cmp.AllowUnexported(Task{}),               // allow typename, payload fields to be compared
	cmpopts.IgnoreFields(Task{}, "opts", "w"), // ignore opts, w fields
}
