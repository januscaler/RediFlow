# RediFlow

A clean simple wrapper targeted at redis streams and redis List and basic pub-sub functionality in a very intuitive class based api

## Usage

```ts
const rediFlow = new RediFlow({});
const stream = rediFlow.createStream('mystream');

const replica2 = await stream.createConsumerGroup(stream.streamName, 'replica-2', { startId: '$' });
const replica1 = await stream.createConsumerGroup(stream.streamName, 'replica-1', { startId: '$' });

const replica1Observable = await replica1.observeStream(1);
const replica2Observable = await replica2.observeStream(1);

replica1Observable.subscribe(async ({ ids, data }) => {
	console.log({ handledBy: 'replica-1', data, ids });
	await replica1.acknowledge(ids);
	await stream.delete(ids);
});

replica2Observable.subscribe(async ({ ids, data }) => {
	console.log({ handledBy: 'replica-2', data, ids });
	await replica1.acknowledge(ids);
	await stream.delete(ids);
});

await stream.addToStream({ cool: { boom: 1 } }, '*');
await stream.addToStream({ cool: { boom: 2 } }, '*');
await stream.addToStream({ cool: { boom: 3 } }, '*');
await stream.addToStream({ cool: { boom: 4 } }, '*');
await stream.addToStream({ cool: { boom: 5 } }, '*');
await stream.addToStream({ cool: { boom: 6 } }, '*');
```

## LICENSE

MIT
