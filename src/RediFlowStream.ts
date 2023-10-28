import _ from 'lodash'
import Redis from 'ioredis'
import { RediFlowConsumerGroup } from './RediFlowConsumerGroup'

export class RediFlowStream {
  protected connection: Redis
  streamName: string
  constructor(streamName: string, connection: Redis) {
    this.connection = connection
    this.streamName = streamName
  }
  async createConsumerGroup(
    groupName: string,
    consumerName: string,
    { makeStream, startId }: { makeStream?: boolean; startId: string | '$' | '0' } = {
      startId: '$',
      makeStream: true,
    }
  ) {
    const consumerGroup = new RediFlowConsumerGroup(groupName, consumerName, this.streamName, this.connection)
    const args: any[] = makeStream ? ['MKSTREAM'] : []
    try {
      const createdGroup = await this.connection.xgroup('CREATE', this.streamName, groupName, startId, ...args)
    } catch (error) {}
    return consumerGroup
  }
  getLength() {
    return this.connection.xlen(this.streamName)
  }
  delete(id: string[]) {
    return this.connection.xdel(this.streamName, ...id)
  }
  trimByMaxLength(maxLength: number) {
    return this.connection.xtrim(this.streamName, 'MAXLEN', '~', maxLength)
  }
  trimByMinId(minId: string) {
    return this.connection.xtrim(this.streamName, 'MINID', '=', minId)
  }
  getRange({ start, end, count, reverse }: { start: string; end: string; count?: number; reverse?: boolean }) {
    const func = reverse ? 'xrevrange' : 'xrange'
    if (_.isNil(count)) return this.connection[func](this.streamName, start, end)
    return this.connection[func](this.streamName, start, end, 'COUNT', count)
  }
  addToStream(map: Record<string, any>, id = '*') {
    const properties = _.transform<any, any>(
      map,
      (properties, value, key) => {
        properties.push(key, _.isObjectLike(value) ? JSON.stringify(value) : value)
      },
      []
    )
    return this.connection.xadd(this.streamName, id, ...properties)
  }
}
