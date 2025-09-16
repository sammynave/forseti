import { describe, expect, it } from 'vitest';
import { Circuit } from './circuit.js';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';

describe('Circuit', () => {
	it('addOperator adds operator to circuit', () => {
		const circuit = new Circuit();
		const result = circuit.addOperator((stream) => stream.negate());

		// Should return circuit for chaining
		expect(result).toBe(circuit);
		expect(circuit.debug()).toBe('Circuit with 1 operators');
	});

	it('execute runs operators in sequence', () => {
		const circuit = new Circuit();
		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 1);
		input.append(zset);

		circuit
			.addOperator((stream) => stream.negate()) // First negate
			.addOperator((stream) => stream.negate()); // Then negate again (back to original)

		const result = circuit.execute(input);
		expect(result.get(0).materialize).toEqual(['test']);
	});

	it('makeIncremental creates incremental version', () => {
		const circuit = new Circuit();
		circuit.addOperator((stream) => stream.liftFilter((item) => item.keep === true));

		const incrementalCircuit = circuit.makeIncremental();

		// Should create new circuit
		expect(incrementalCircuit).not.toBe(circuit);
		expect(incrementalCircuit.debug()).toBe('Circuit with 1 operators');
	});

	it('addFeedback creates feedback loop', () => {
		const circuit = new Circuit();
		const feedbackCircuit = circuit.addFeedback((stream) => stream.negate());

		expect(feedbackCircuit).not.toBe(circuit);
		expect(feedbackCircuit.debug()).toBe('Circuit with 1 operators');
	});

	it('addBinaryOperator handles two streams', () => {
		const circuit = new Circuit();
		const rightStream = new Stream();
		const rightZSet = new ZSet();
		rightZSet.add('right', 1);
		rightStream.append(rightZSet);

		circuit.addBinaryOperator((left, right) => left.plus(right), rightStream);

		const leftStream = new Stream();
		const leftZSet = new ZSet();
		leftZSet.add('left', 1);
		leftStream.append(leftZSet);

		const result = circuit.execute(leftStream);
		const materialized = result.get(0).materialize;
		expect(materialized).toContain('left');
		expect(materialized).toContain('right');
	});

	it('addNaryOperator handles multiple streams', () => {
		const circuit = new Circuit();
		const stream2 = new Stream();
		const stream3 = new Stream();

		const zset2 = new ZSet();
		zset2.add('stream2', 1);
		stream2.append(zset2);

		const zset3 = new ZSet();
		zset3.add('stream3', 1);
		stream3.append(zset3);

		circuit.addNaryOperator((s1, s2, s3) => s1.plus(s2).plus(s3), stream2, stream3);

		const stream1 = new Stream();
		const zset1 = new ZSet();
		zset1.add('stream1', 1);
		stream1.append(zset1);

		const result = circuit.execute(stream1);
		const materialized = result.get(0).materialize;
		expect(materialized).toContain('stream1');
		expect(materialized).toContain('stream2');
		expect(materialized).toContain('stream3');
	});

	it('addRecursiveOperator handles recursion', () => {
		const circuit = new Circuit();
		let callCount = 0;

		circuit.addRecursiveOperator(
			(stream) => {
				callCount++;
				return stream; // Identity for simplicity
			},
			(stream) => callCount >= 3 // Terminate after 3 calls
		);

		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 1);
		input.append(zset);

		const result = circuit.execute(input);
		expect(callCount).toBe(3);
	});

	it('debug returns circuit information', () => {
		const circuit = new Circuit();
		expect(circuit.debug()).toBe('Circuit with 0 operators');

		circuit.addOperator((stream) => stream);
		expect(circuit.debug()).toBe('Circuit with 1 operators');
	});

	it('asFunction returns executable function', () => {
		const circuit = new Circuit();
		circuit.addOperator((stream) => stream.negate());

		const fn = circuit.asFunction();
		expect(typeof fn).toBe('function');

		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 1);
		input.append(zset);

		const result = fn(input);
		expect(result.get(0).materialize).toEqual([]);
	});

	it('addDBSPFeedback implements proper DBSP feedback semantics', () => {
		const circuit = new Circuit();

		// Create a feedback circuit: output = input + delayed_negated_output
		// This should converge to a fixed point
		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) => stream.negate() // F(α) = -α, applied to delayed stream
		);

		const input = new Stream();

		// t=0: input has value 5
		const zset0 = new ZSet();
		zset0.add('item', 5);
		input.append(zset0);

		// t=1: input has value 3
		const zset1 = new ZSet();
		zset1.add('item', 3);
		input.append(zset1);

		const result = feedbackCircuit.execute(input);

		// At t=0, should be just the input (since feedback is 0)
		expect(result.get(0).materialize).toEqual(['item']);
		// At t=0: output = input + 0 (no feedback yet) = 5
		expect(result.get(0).debug().get('"item"')).toBe(5);

		// At t=1: output = input + F(delayed_output) = 3 + (-5) = -2
		expect(result.get(1).debug().get('"item"')).toBe(-2);
	});

	it('addDBSPFeedback handles fixed point computation', () => {
		const circuit = new Circuit();

		// Create feedback where output converges
		// Let's use a positive feedback that actually converges to a non-zero value
		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) =>
				stream.lift((zset) => {
					const result = new ZSet();
					for (const [key, weight] of zset.debug()) {
						const item = JSON.parse(key);
						result.add(item, weight * 0.5); // Positive feedback with damping
					}
					return result;
				})
		);

		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 10);
		input.append(zset);

		// Add more time steps to see convergence
		for (let i = 1; i < 10; i++) {
			input.append(new ZSet()); // Zero input after t=0
		}

		const result = feedbackCircuit.execute(input);

		// With positive feedback 0.5:
		// output[0] = 10
		// output[1] = 0 + 0.5*10 = 5
		// output[2] = 0 + 0.5*5 = 2.5
		// output[3] = 0 + 0.5*2.5 = 1.25
		// Converges to 0

		// Or better yet, test the mathematical property directly:
		const finalWeight = result.get(9).debug().get('"test"');
		expect(Math.abs(finalWeight)).toBeLessThan(0.1); // Should converge to ~0
	});

	it('addDBSPFeedback converges to zero with decaying input', () => {
		const circuit = new Circuit();

		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) =>
				stream.lift((zset) => {
					const result = new ZSet();
					for (const [key, weight] of zset.debug()) {
						const item = JSON.parse(key);
						result.add(item, weight * 0.5);
					}
					return result;
				})
		);

		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 10);
		input.append(zset);

		// Zero input after t=0
		for (let i = 1; i < 10; i++) {
			input.append(new ZSet());
		}

		const result = feedbackCircuit.execute(input);

		// Should be converging toward 0
		const early = result.get(5).debug().get('"test"');
		const late = result.get(9).debug().get('"test"');

		// Later values should be closer to 0
		expect(Math.abs(late)).toBeLessThan(Math.abs(early));

		// Should be very close to 0 by t=9
		expect(Math.abs(late)).toBeLessThan(0.1);
	});

	it('addDBSPFeedback shows geometric decay', () => {
		const circuit = new Circuit();

		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) =>
				stream.lift((zset) => {
					const result = new ZSet();
					for (const [key, weight] of zset.debug()) {
						const item = JSON.parse(key);
						result.add(item, weight * 0.5);
					}
					return result;
				})
		);

		const input = new Stream();
		const zset = new ZSet();
		zset.add('test', 8);
		input.append(zset);

		input.append(new ZSet()); // Zero at t=1

		const result = feedbackCircuit.execute(input);

		// t=0: 8, t=1: 0 + 0.5*8 = 4
		expect(result.get(0).debug().get('"test"')).toBe(8);
		expect(result.get(1).debug().get('"test"')).toBe(4);
	});

	it('addDBSPFeedback basic functionality', () => {
		const circuit = new Circuit();

		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) => stream.negate()
		);

		const input = new Stream();
		const zset1 = new ZSet();
		zset1.add('test', 5);
		input.append(zset1);

		const zset2 = new ZSet();
		zset2.add('test', 3);
		input.append(zset2);

		const result = feedbackCircuit.execute(input);

		// t=0: output = 5 + 0 = 5
		expect(result.get(0).debug().get('"test"')).toBe(5);

		// t=1: output = 3 + (-5) = -2
		expect(result.get(1).debug().get('"test"')).toBe(-2);
	});

	it('addDBSPFeedback shows expected feedback behavior', () => {
		const circuit = new Circuit();

		const feedbackCircuit = circuit.addDBSPFeedback(
			(input, feedback) => input.plus(feedback),
			(stream) =>
				stream.lift((zset) => {
					const result = new ZSet();
					for (const [key, weight] of zset.debug()) {
						const item = JSON.parse(key);
						result.add(item, weight * 0.5); // Simple 0.5 multiplier
					}
					return result;
				})
		);

		const input = new Stream();
		const zset1 = new ZSet();
		zset1.add('test', 4);
		input.append(zset1);

		const zset2 = new ZSet();
		input.append(zset2); // Zero input at t=1

		const result = feedbackCircuit.execute(input);

		// t=0: output = 4 + 0 = 4
		expect(result.get(0).debug().get('"test"')).toBe(4);

		// t=1: output = 0 + 0.5*4 = 2
		expect(result.get(1).debug().get('"test"')).toBe(2);
	});
});

describe('Circuit Integration', () => {
	it('chains multiple operators correctly', () => {
		const circuit = new Circuit();
		const input = new Stream();

		const zset = new ZSet();
		zset.add({ value: 5 }, 1);
		zset.add({ value: 10 }, 1);
		input.append(zset);

		circuit
			.addOperator((stream) => stream.liftFilter((item) => item.value > 7))
			.addOperator((stream) => stream.liftProject((item) => ({ doubled: item.value * 2 })));

		const result = circuit.execute(input);
		const materialized = result.get(0).materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual({ doubled: 20 });
	});
});
