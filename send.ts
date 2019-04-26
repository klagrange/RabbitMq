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
      tryAttemp: number;
      isValid: boolean;
      body: {
        name: string;
        age: number;
      }
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
              opts: { headers: { 'x-delay': 200 }}
            },
            consume: {
              opts: { noAck: false }
            }
          },
          messageFormat: { 
            tryAttemp: 1,
            isValid: false,
            body: {
              name: 'example',
              age: 0 
            }
          }
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
        const res = await ch.publish(
          action.exchange.name,
          action.key,
          Buffer.from(JSON.stringify(msgObj)),
          action.publish.opts)
        return res;
      }
      return { clean, publish };
    }

    async function consumer(self: Newbie) {
      const { publish, clean: cleanPublishCh } = await publisher(self);
      const { ch, clean: cleanConsumeCh } = await self.getCh();
      await ch.assertQueue(action.key, action.queue.opts);
      await ch.bindQueue(action.key, action.exchange.name, action.key);
      const consume = async (onReceive: (res: amqp.ConsumeMessage) => any) => {
        await ch.consume(action.key, onReceive, action.consume.opts);
      }
      return { ch, consume, cleanConsumeCh, publish, cleanPublishCh };
    }

    return await consumer(this);
  }

  async watchRds() {
    const { action, messageFormat } = this.ACTIONS.WATCH_RDS;
    const x = await this.getX(action);

    const publish = async (msgObj: typeof messageFormat) => {
      await x.publish(msgObj);
    }

    const consume = async (isRdsReady: (msg: any) => boolean) => {
      await x.consume(async (res) => {
        const msg: typeof messageFormat = JSON.parse(res.content.toString());

        // ack original msg
        x.ch.ack(res);

        const isValid = isRdsReady(msg);

        // resend
        if (!isValid && msg.tryAttemp < 5) {
          await publish({
            ...msg,
            tryAttemp: msg.tryAttemp + 1,
            isValid: false
          })
        } else {
          await x.cleanPublishCh();
        }
      })
    }
    return { ...x, publish, consume };
  }
}

async function run() {
  const newbie = new Newbie('amqp://localhost');
  const { publish, consume } = await newbie.watchRds();

  publish({
    tryAttemp: 1,
    isValid: false,
    body: {
      name: 'Keith',
      age: 25
    }    
  })

  await consume((msg => {
    console.log(msg);
    return true;
  }));
}

run();
