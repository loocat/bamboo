use crossflow

db.createUser(
  {
    user: "root",
    pwd: "myroot",
    roles: [
      { role: "dbOwner", db: "crossflow" }
    ]
  }
)

db.resource.ensureIndex(
  { "resourceid": 1, }, { unique: true }
)

db.resource.ensureIndex(
  { "path": 1 }, { unique: true }
)

db.resource.ensureIndex(
  { "parentpath": 1, "resourcetype": 1, "creationtime": -1 }
)

db.resource.ensureIndex(
  { "creationtime": -1 }
)

db.resource.ensureIndex(
  { "lastmodifiedtime": -1 }
)

db.resource.ensureIndex(
  { "expirationtime": -1 }
)