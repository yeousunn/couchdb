defmodule PartitionViewUpdateTest do
  use CouchTestCase
  import PartitionHelpers

  @moduledoc """
  Test Partition view update functionality
  """
  @tag :with_partitioned_db
  test "view updates properly remove old keys", context do
    db_name = context[:db_name]
    create_partition_docs(db_name, "foo", "bar")
    create_partition_ddoc(db_name)

    check_key = fn key, num_rows ->
      url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
      resp = Couch.get(url, query: [key: key])
      assert resp.status_code == 200
      assert length(resp.body["rows"]) == num_rows
    end

    check_key.(2, 1)

    resp = Couch.get("/#{db_name}/foo:2")
    doc = Map.put(resp.body, "value", 4)
    resp = Couch.put("/#{db_name}/foo:2", query: [w: 3], body: doc)
    assert resp.status_code >= 201 and resp.status_code <= 202

    check_key.(4, 2)
    check_key.(2, 0)
  end

  @tag :with_partitioned_db
  test "query with update=false works", context do
    db_name = context[:db_name]
    create_partition_docs(db_name)
    create_partition_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp =
      Couch.get(url,
        query: %{
          update: "true",
          limit: 3
        }
      )

    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:2", "foo:4", "foo:6"]

    # Avoid race conditions by attempting to get a full response
    # from every shard before we do our update:false test
    for _ <- 1..12 do
      resp = Couch.get(url)
      assert resp.status_code == 200
    end

    Couch.put("/#{db_name}/foo:1", body: %{some: "field"})

    resp =
      Couch.get(url,
        query: %{
          update: "false",
          limit: 3
        }
      )

    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:2", "foo:4", "foo:6"]
  end
end
