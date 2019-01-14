defmodule PartitionHelpers do
  use ExUnit.Case
  use CouchTestCase

  def create_partition_docs(db_name, pk1 \\ "foo", pk2 \\ "bar") do
    docs =
      for i <- 1..100 do
        id =
          if rem(i, 2) == 0 do
            "#{pk1}:#{i}"
          else
            "#{pk2}:#{i}"
          end

        group =
          if rem(i, 3) == 0 do
            "one"
          else
            "two"
          end

        %{
          :_id => id,
          :value => i,
          :some => "field",
          :group => group
        }
      end

    resp = Couch.post("/#{db_name}/_bulk_docs", body: %{:w => 3, :docs => docs})
    assert resp.status_code == 201
  end

  def create_partition_ddoc(db_name, opts \\ %{}) do
    map_fn = """
      function(doc) {
        if (doc.some) {
          emit(doc.value, doc.some);
        }
      }
    """

    default_ddoc = %{
      views: %{
        some: %{
          map: map_fn
        }
      }
    }

    ddoc = Enum.into(opts, default_ddoc)

    resp = Couch.put("/#{db_name}/_design/mrtest", body: ddoc)
    assert resp.status_code == 201
    assert Map.has_key?(resp.body, "ok") == true
  end

  def crud_partition_doc(db_name, pk \\ "foo") do
    resp = Couch.post("/#{db_name}", body: %{:_id => "#{pk}:id1", :a => 1, :b => 1}).body
    assert resp["ok"]

    url = "/#{db_name}/_partition/#{pk}"
    resp = Couch.get(url)

    assert resp.status_code == 200
    %{:body => body} = resp
    assert body["doc_count"] == 1
    assert body["doc_del_count"] == 0
    assert body["partition"] == "#{pk}"
    exernal_size1 = body["sizes"]["external"]
    assert exernal_size1 > 0

    resp = Couch.post("/#{db_name}", body: %{:_id => "#{pk}:id2", :a => 2, :b => 22}).body
    rev = resp["rev"]
    assert resp["ok"]

    url = "/#{db_name}/_partition/#{pk}"
    resp = Couch.get(url)

    assert resp.status_code == 200
    %{:body => body} = resp
    assert body["doc_count"] == 2
    assert body["doc_del_count"] == 0
    assert body["partition"] == "#{pk}"
    exernal_size2 = body["sizes"]["external"]
    assert exernal_size2 > exernal_size1

    resp = Couch.delete("/#{db_name}/#{pk}:id2?rev=#{rev}").body
    assert resp["ok"]
    :timer.sleep(300)

    url = "/#{db_name}/_partition/#{pk}"
    resp = Couch.get(url)

    assert resp.status_code == 200
    %{:body => body} = resp
    assert body["doc_count"] == 1
    assert body["doc_del_count"] == 1
    assert body["partition"] == "#{pk}"
    exernal_size3 = body["sizes"]["external"]
    assert exernal_size3 <= exernal_size2

    compact(db_name)

    url = "/#{db_name}/_partition/#{pk}"
    resp = Couch.get(url)
    %{:body => body} = resp
    exernal_size4 = body["sizes"]["external"]
    assert exernal_size4 < exernal_size3

    map = ~s"""
    function (doc) {
      emit(doc.integer, doc.integer);
      emit(doc.integer, doc.integer);
    };
    """
    red_doc = %{:views => %{:bar => %{:map => map}}}

    assert Couch.put("/#{db_name}/_design/#{pk}_foo", body: red_doc).body["ok"]

    url = "/#{db_name}/_partition/#{pk}"
    resp = Couch.get(url)

    assert resp.status_code == 200
    %{:body => body} = resp
    assert body["doc_count"] == 1
    assert body["doc_del_count"] == 1
    assert body["partition"] == "#{pk}"
    exernal_size5 = body["sizes"]["external"]
    assert exernal_size5 == exernal_size4
  end

  def get_ids(resp) do
    %{:body => %{"rows" => rows}} = resp
    Enum.map(rows, fn row -> row["id"] end)
  end

  def get_partitions(resp) do
    %{:body => %{"rows" => rows}} = resp

    Enum.map(rows, fn row ->
      [partition, _] = String.split(row["id"], ":")
      partition
    end)
  end

  def assert_correct_partition(partitions, correct_partition) do
    assert Enum.all?(partitions, fn partition ->
             partition == correct_partition
           end)
  end

  def compact(db) do
    assert Couch.post("/#{db}/_compact").status_code == 202

    retry_until(
      fn ->
        Couch.get("/#{db}").body["compact_running"] == false
      end,
      200,
      20_000
    )
  end
end
