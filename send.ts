import * as amqp from 'amqplib';

interface IAction {
  readonly exchange: {
    readonly name: string,
    readonly type: string,
    readonly opts: amqp.Options.AssertExchange
  },
  readonly key: string,
  readonly queue: {
    readonly opts: amqp.Options.AssertQueue
  }
  readonly publish: {
    readonly opts: amqp.Options.Publish
  },
  readonly consume: {
    readonly opts: amqp.Options.Consume
  }
}

interface IActions {
  TEST: {
    readonly action: IAction,
    readonly messageFormat: {
      name: string;
      age: number;
    }
  },
  WATCH_RDS: {
    readonly action: IAction,
    readonly messageFormat: {
      name: string;
      age: number;
    }
  },}

class Newbie {
  readonly amqpConn: string;
  readonly ACTIONS: IActions;

  constructor(amqpConn: string) {
    this.amqpConn = amqpConn;
    this.ACTIONS = {
        'TEST': {
          action: {
            exchange: {
              name: 'TEST',
              type: 'direct',
              opts: {}
            },
            key: 'TEST',
            queue: {
              opts: { durable: true }
            },
            publish: {
              opts: {}
            },
            consume: {
              opts: { noAck: true }
            }
          },
          messageFormat: { name: 'example', age: 0 }
        },
        'WATCH_RDS': {
          action: {
            exchange: {
              name: 'WATCH_RDS',
              type: 'x-delayed-message',
              opts: { durable: true, arguments: { 'x-delayed-type': 'direct' }}
            },
            key: 'WATCH_RDS',
            queue: {
              opts: { durable: true }
            },
            publish: {
              opts: { headers: { 'x-delay': 5000 }}
            },
            consume: {
              opts: { noAck: false }
            }
          },
          messageFormat: { name: 'example', age: 0 }
        },
    }      
  }

  private async getCh() {
    const conn = await amqp.connect(this.amqpConn);
    const ch = await conn.createChannel();
    const clean = async () => {
      await ch.close();
      await conn.close();
    }
    return { ch, clean };
  }
 
  private async getX(action: IAction) {

    async function publisher(self: Newbie) {
      const { ch, clean } = await self.getCh();
      await ch.assertExchange(
        action.exchange.name,
        action.exchange.type,
        action.exchange.opts);
      const publish = async (msgObj: Object) => {
        await ch.publish(
          action.exchange.name,
          action.key,
          Buffer.from(JSON.stringify(msgObj)),
          action.publish.opts)
      }
      return { clean, publish };
    }

    async function consumer(self: Newbie) {
      const { ch, clean } = await self.getCh();
      await ch.assertQueue(action.key, action.queue.opts);
      await ch.bindQueue(action.key, action.exchange.name, action.key);
      const consume = async (callback: (res: amqp.ConsumeMessage, msg: any) => any) => {
        await ch.consume(action.key, (res) => {
          const msg = JSON.parse(res.content.toString());
          callback(res, msg);
        }, action.consume.opts);
      }
      return { consume, clean };
    }

    return { 
      publisher: await publisher(this),
      consumer: await consumer(this),
    }
  }

  async test() {
    const { action, messageFormat } = this.ACTIONS.TEST;
    const { publisher, consumer } = await this.getX(action);

    const publish = async (msgObj: typeof messageFormat) => {
      await publisher.publish(msgObj);
    }

    const consume = async (callback: (res: amqp.ConsumeMessage, msg: typeof messageFormat) => any) => {
      await consumer.consume(callback);
    }

    return {
      publisher: {...publisher, publish },
      consumer: {...consumer, consume }
    }
  }

  async watchRds() {
    const { action, messageFormat } = this.ACTIONS.WATCH_RDS;
    const { publisher, consumer } = await this.getX(action);

    const publish = async (msgObj: typeof messageFormat) => {
      await publisher.publish(msgObj);
    }

    const consume = async (callback: (res: amqp.ConsumeMessage, msg: typeof messageFormat) => any) => {
      await consumer.consume(callback);
    }

    return {
      publisher: {...publisher, publish },
      consumer: {...consumer, consume }
    }
  }

}

async function run() {
  const newbie = new Newbie('amqp://localhost');
  const test = await newbie.watchRds();

  test.consumer.consume((res, msg) => {
    console.log(res);
    console.log(msg)
  });

  // test.publisher.publish({
  //   name: 'Keith',
  //   age: 1
  // });

  // test.publisher.publish({
  //   name: 'Keith',
  //   age: 2
  // });
}

run();
