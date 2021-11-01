package raft

//func TestTermStash_Get_Empty(t *testing.T) {
//ctx := common.NewContext(common.NewEmptyConfig())
//defer ctx.Close()

//terms := OpenTestTermStash(ctx)
//term, err := terms.Get(uuid.NewV1())
//assert.Nil(t, err)
//assert.Zero(t, term)
//}

//func TestTermStash_PutGet_NoLeader_NoVote(t *testing.T) {
//ctx := common.NewContext(common.NewEmptyConfig())
//defer ctx.Close()

//terms := OpenTestTermStash(ctx)

//id := uuid.NewV1()
//exp := term{1, nil, nil}
//err := terms.Save(id, exp)
//assert.Nil(t, err)

//actual, err := terms.Get(id)
//assert.Nil(t, err)
//assert.Equal(t, exp, actual)
//}

//func TestTermStash_PutGet_NoLeader(t *testing.T) {
//ctx := common.NewContext(common.NewEmptyConfig())
//defer ctx.Close()

//terms := OpenTestTermStash(ctx)

//id := uuid.NewV1()
//exp := term{1, nil, &id}
//err := terms.Save(id, exp)
//assert.Nil(t, err)

//actual, err := terms.Get(id)
//assert.Nil(t, err)
//assert.Equal(t, exp, actual)
//}

//func TestTermStash_PutGet_NoVote(t *testing.T) {
//ctx := common.NewContext(common.NewEmptyConfig())
//defer ctx.Close()

//terms := OpenTestTermStash(ctx)

//id := uuid.NewV1()
//exp := term{1, &id, nil}
//err := terms.Save(id, exp)
//assert.Nil(t, err)

//actual, err := terms.Get(id)
//assert.Nil(t, err)
//assert.Equal(t, exp, actual)
//}

//func TestTermStash_PutGet_LeaderAndVote(t *testing.T) {
//ctx := common.NewContext(common.NewEmptyConfig())
//defer ctx.Close()

//terms := OpenTestTermStash(ctx)

//id := uuid.NewV1()
//exp := term{1, &id, &id}
//err := terms.Save(id, exp)
//assert.Nil(t, err)

//actual, err := terms.Get(id)
//assert.Nil(t, err)
//assert.Equal(t, exp, actual)
//}

//func OpenTestStash(ctx common.Context) *bolt.DB {
//db, err := stash.OpenTransient(ctx)
//if err != nil {
//panic(err)
//}
//return db
//}

//func OpenTestTermStash(ctx common.Context) *termStore {
//ret, err := openTermStore(OpenTestStash(ctx))
//if err != nil {
//panic(err)
//}
//return ret
//}
