import _ from 'lodash';
import Redis from 'ioredis';
import { Subject } from 'rxjs';

export class RediFlowConsumerGroup {
	protected connection: Redis;
	protected groupName: string;
	protected streamName: string;
	protected consumerName: string;
	constructor(groupName: string, consumerName: string, streamName: string, connection: Redis) {
		this.connection = connection;
		this.groupName = groupName;
		this.streamName = streamName;
		this.consumerName = consumerName;
	}
	createConsumer(consumer: string) {
		return this.connection.duplicate().xgroup('CREATECONSUMER', this.streamName, this.groupName, consumer);
	}
	deleteConsumer(consumer: string) {
		return this.connection.duplicate().xgroup('DELCONSUMER', this.streamName, this.groupName, consumer);
	}
	destroy() {
		return this.connection.duplicate().xgroup('DESTROY', this.streamName, this.groupName);
	}
	readGroup({ count, block, id }: { id: string; count?: number; block?: number }) {
		if (!_.isNil(count) && !_.isNil(block)) {
			return this.connection.duplicate().xreadgroup('GROUP', this.groupName, this.consumerName, 'COUNT', count, 'BLOCK', block, 'STREAMS', this.streamName, id);
		} else if (_.isNil(count) && !_.isNil(block)) {
			return this.connection.duplicate().xreadgroup('GROUP', this.groupName, this.consumerName, 'BLOCK', block, 'STREAMS', this.streamName, id);
		} else if (!_.isNil(count) && _.isNil(block)) {
			return this.connection.duplicate().xreadgroup('GROUP', this.groupName, this.consumerName, 'COUNT', count, 'STREAMS', this.streamName, id);
		}
		return this.connection.duplicate().xreadgroup('GROUP', this.groupName, this.consumerName, 'STREAMS', this.streamName, id);
	}
	protected pairedArrayToObject(array: string[]) {
		const newMessage = _.fromPairs(_.chunk(array, 2));
		const keyValueObject = _.isArray(newMessage) ? { [newMessage[0]]: newMessage[1] } : newMessage;
		return keyValueObject;
	}
	async observeStream({ count, block }: { count?: number; block?: number } = { block: 0, count: 1 }) {
		const observable = new Subject<{
			key: string;
			ids: string[];
			items: Record<string, any>[];
		}>();
		let isFetching = true;
		const self = this;
		async function fetchItems() {
			if (observable.closed) {
				stopObserving();
			}
			try {
				const streamData: any = await self.connection
					.duplicate()
					.xreadgroup('GROUP', self.groupName, self.consumerName, 'COUNT', count, 'BLOCK', block, 'STREAMS', self.streamName, '>');
				if (_.isNil(streamData)) {
					return;
				}
				const [key, messages] = streamData[0];
				const ids = _.map(messages, (message) => message[0]);
				const messageItems = _.map(messages, (message) => message[1]);
				const result = _.map(messageItems, self.pairedArrayToObject);
				observable.next({ key, ids, items: result });
				setTimeout(async () => {
					if (isFetching) {
						await fetchItems();
					}
				}, 0);
			} catch (error) {
				console.log(error);
			}
		}
		const interval = setTimeout(async () => {
			fetchItems();
		}, 0);

		process.on('exit', (code) => {
			stopObserving();
			process.exit(0);
		});
		process.on('SIGINT', () => {
			stopObserving();
			process.exit(0);
		});
		function stopObserving() {
			isFetching = false;
			clearTimeout(interval);
			observable.complete();
		}
		return observable;
	}
	acknowledge(id: string[]) {
		return this.connection.duplicate().xack(this.streamName, this.groupName, ...id);
	}
}
