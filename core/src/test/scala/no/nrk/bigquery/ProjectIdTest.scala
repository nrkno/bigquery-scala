package no.nrk.bigquery

class ProjectIdTest extends munit.FunSuite {

  test("valid projectId") {
    assert(ProjectId.fromString("abc-def").isRight)
    assert(ProjectId.fromString("abcdef").isRight)
    assert(ProjectId.fromString("abcdef123").isRight)
    assert(ProjectId.fromString("abcdef-123").isRight)
  }

  test("invalid projectId") {
    assert(ProjectId.fromString("abc-def-").isLeft)
    assert(ProjectId.fromString("1abcdef_").isLeft)
    assert(ProjectId.fromString("1fffffff").isLeft)
    assert(ProjectId.fromString("zxyvx").isLeft)
    assert(ProjectId.fromString("1zxyvxx").isLeft)
    assert(ProjectId.fromString("abcdefghijklmnopqrstuvwxyz12345").isLeft)
  }
}
