import { describe, expect, it } from 'vitest';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { integrate } from './stream/utils.js';

describe('Debug Theorem 3.4', () => {
	it('debug incremental join step by step', () => {
		const a = new Stream();
		const b = new Stream();

		const za = new ZSet();
		za.add({ id: 1, x: 'A' }, 1);
		const zb = new ZSet();
		zb.add({ id: 1, y: 'B' }, 1);
		a.append(za);
		b.append(zb);

		console.log('=== INPUT STREAMS ===');
		console.log('Stream a:', a.get(0).materialize);
		console.log('Stream b:', b.get(0).materialize);

		// Manual calculation of (a × b)^Δ = I(a) × b + a × z^(-1)(I(b))
		const integratedA = integrate(a);
		const integratedB = integrate(b);
		const delayedIntegratedB = integratedB.delay();

		console.log('=== INTERMEDIATE RESULTS ===');
		console.log('I(a)[0]:', integratedA.get(0).materialize);
		console.log('I(b)[0]:', integratedB.get(0).materialize);
		console.log('z^(-1)(I(b))[0]:', delayedIntegratedB.get(0).materialize);

		const term1 = integratedA.liftJoin(
			b,
			(x) => x.id,
			(y) => y.id
		);
		const term2 = a.liftJoin(
			delayedIntegratedB,
			(x) => x.id,
			(y) => y.id
		);

		console.log('=== TERMS ===');
		console.log('Term1 (I(a) × b)[0]:', term1.get(0).materialize);
		console.log('Term2 (a × z^(-1)(I(b)))[0]:', term2.get(0).materialize);

		const expected = term1.plus(term2);
		console.log('Expected result[0]:', expected.get(0).materialize);

		const actual = a.liftJoinIncremental(
			b,
			(x) => x.id,
			(y) => y.id
		);
		console.log('Actual result[0]:', actual.get(0).materialize);

		// Let's also check the debug info
		console.log('=== DEBUG INFO ===');
		console.log('Expected debug:', expected.get(0).debug());
		console.log('Actual debug:', actual.get(0).debug());

		// Add proper assertion to verify they match
		expect(actual.get(0).debug()).toEqual(expected.get(0).debug());
	});
});
