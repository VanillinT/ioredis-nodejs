const Redis = require("ioredis");

let redis = new Redis();
// clear cache to simplify data processing
redis.flushall();
// log the port redis launches on
console.log(redis.options.port);

class Data {
  constructor(id, value, info) {
    this.id = id;
    this.value = value;
    this.info = info;
  }
}
class Message {
  constructor(msg) {
    this.msg = msg;
    this.time = new Date().getTime();
  }
}

let dataArr = [
  new Data("id1", "val1", "info1"),
  new Data("id2", "val2", "info2"),
  new Data("id3", "val3", "info3"),
];
let messageArr = [
  new Message("Hello World"),
  new Message("foo bar"),
  new Message("random text"),
];

// returns unique names for consumers
let consCount = 0;
let newConsId = () => "consumer_" + consCount++;

// create a group with streams and fill streams with data
async function init() {
  await redis.xgroup("CREATE", "foo", "newgroup", "$", "MKSTREAM");
  await redis.xgroup("CREATE", "bar", "newgroup", "$", "MKSTREAM");

  for (let data of dataArr) {
    redis.xadd(
      "foo",
      "*",
      "id",
      data.id,
      "value",
      data.value,
      "info",
      data.info
    );
  }
  for (let msg of messageArr) {
    redis.xadd("bar", "*", "message", msg.msg, "timestamp", msg.time);
  }

  // log the contents of both streams
  redis.xrange("foo", "-", "+").then((res) => console.log("xrange foo", res));
  redis.xrange("bar", "-", "+").then((res) => console.log("xrange bar", res));
}

// read defined streams as a consumer, getting 1 unprocessed value
async function readAsConsumer(consumer, streams) {
  let ids = [];
  for (let i of streams) ids.push(">");
  return redis
    .xreadgroup(
      "GROUP",
      "newgroup",
      consumer,
      "COUNT",
      1,
      "STREAMS",
      ...streams,
      ...ids
    )
    .then(
      (res) => {
        console.log(JSON.stringify(res));
      },
      (rej) => console.log(rej)
    );
}
async function main() {
  let cons1 = newConsId(),
    cons2 = newConsId(),
    cons3 = newConsId();
  await init().catch((err) => {
    console.log(err);
  });
  
  await readAsConsumer(cons1, ["foo"]); // "foo",[["1597048954034-0",["id","id1","value","val1","info","info1"]]]
  await readAsConsumer(cons2, ["bar"]); // "bar",[["1597048954034-0",["message","Hello World","timestamp","1597048954013"]]]

  // "foo",[["1597048954034-1",["id","id2","value","val2","info","info2"]]]
  // "bar",[["1597048954034-1",["message","foo bar","timestamp","1597048954013"]]]
  await readAsConsumer(cons3, ["foo", "bar"]);
}

main().catch((ex) => console.error(ex));
