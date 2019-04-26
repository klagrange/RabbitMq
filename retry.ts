import * as amqp from 'amqplib';

async function getCh() {
  const conn = await amqp.connect('amqp://localhost');
  const ch = await conn.createChannel();
  const clean = async () => {
    await ch.close();
    await conn.close();
  }
  return { ch, clean };
}

async function publisher() {
  const { ch, clean } = await getCh();
  await ch.assertExchange(
      'PIKACHU',
      'x-delayed-message',
      { durable: true, arguments: { 'x-delayed-type': 'direct' }});
  const publish = async (msgObj: Object) => {
      const res =  await ch.publish(
          'PIKACHU',
          'pika',
          Buffer.from(JSON.stringify(msgObj)),
          { headers: { 'x-delay': 1000 }})
      clean();
      return res;
  }
  return { publish };
}

async function consumer() {
  const { publish } = await publisher();
  const { ch, clean } = await getCh();
  await ch.assertQueue('pika', { 
    durable: true
  });
  await ch.bindQueue('pika', 'PIKACHU', 'pika');
  const consume = async () => {
    await ch.consume('pika', (res) => {
      const msg = JSON.parse(res.content.toString());
      console.log(msg);

      // ack original msg
      ch.ack(res);

      // publish another msg
      if(!msg.isValid) {
        publish({
          n: 2,
          tryAttemp: msg.tryAttemp + 1,
          isValid: true,
        })
      }
    }, { noAck: false });
  }
  return { consume, clean };
}

async function run() {
  // const { publish } = await publisher();
  // const a = await publish({
  //   n: 1,
  //   tryAttemp: 1,
  //   isValid: false
  // });
  // console.log(a);

  const { consume } = await consumer();
  await consume();
};

run();

