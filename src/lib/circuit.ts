import type { Stream, StreamOperator } from './stream.js';
import { ZSet, ZSetGroup, TupleGroup } from './z-set.js';
import { ZSetOperators } from './z-set-operators.js';
import { incrementalize as naiveIncrementalize } from './stream.js';
import { lift } from './operators/lift.js';
import {
	completeIncrementalize as incrementalize,
	optimizeBilinear,
	optimizedDistinctIncremental
} from './optimization.js';

export class Circuit<A, B> {
	constructor(private operator: StreamOperator<A, B>) {}

	compose<C>(other: Circuit<B, C>): Circuit<A, C> {
		return new Circuit((input: Stream<A>) => {
			const intermediate = this.operator(input);
			return other.operator(intermediate);
		});
	}

	execute(input: Stream<A>): Stream<B> {
		return this.operator(input);
	}

	// ========== BILINEAR OPERATIONS (need incrementalize) ==========
	static equiJoin<T, U, K>(
		keyA: (t: T) => K,
		keyB: (u: U) => K
	): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = optimizeBilinear(
			(a: ZSet<T>, b: ZSet<U>) => ZSetOperators.equiJoin(a, b, keyA, keyB),
			new ZSetGroup<T>(),
			new ZSetGroup<U>(),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}

	/*
	 * @TODO perf this
	 */
	static oldequiJoin<T, U, K>(
		keyA: (t: T) => K,
		keyB: (u: U) => K
	): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<U>]) => ZSetOperators.equiJoin(a, b, keyA, keyB)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<U>()),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}
	static cartesianProduct<T, U>(): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = optimizeBilinear(
			(a: ZSet<T>, b: ZSet<U>) => ZSetOperators.cartesianProduct(a, b),
			new ZSetGroup<T>(),
			new ZSetGroup<U>(),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}
	static oldcartesianProduct<T, U>(): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<U>]) => ZSetOperators.cartesianProduct(a, b)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<U>()),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}
	static intersect<T>(): Circuit<[ZSet<T>, ZSet<T>], ZSet<T>> {
		const op = optimizeBilinear(
			(a: ZSet<T>, b: ZSet<T>) => ZSetOperators.intersect(a, b),
			new ZSetGroup<T>(),
			new ZSetGroup<T>(),
			new ZSetGroup<T>()
		);
		return new Circuit(op);
	}
	static oldintersect<T>(): Circuit<[ZSet<T>, ZSet<T>], ZSet<T>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<T>]) => ZSetOperators.intersect(a, b)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<T>()),
			new ZSetGroup<T>()
		);
		return new Circuit(op);
	}
	static join<T, U>(
		predicate: (recordA: T, recordB: U) => boolean
	): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = optimizeBilinear(
			(a: ZSet<T>, b: ZSet<U>) => ZSetOperators.join(a, b, predicate),
			new ZSetGroup<T>(),
			new ZSetGroup<U>(),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}
	static oldjoin<T, U>(
		predicate: (recordA: T, recordB: U) => boolean
	): Circuit<[ZSet<T>, ZSet<U>], ZSet<[T, U]>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<U>]) => ZSetOperators.join(a, b, predicate)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<U>()),
			new ZSetGroup<[T, U]>()
		);
		return new Circuit(op);
	}

	// ========== LINEAR OPERATIONS (automatically incremental) ==========

	static filter<T>(predicate: (t: T) => boolean): Circuit<ZSet<T>, ZSet<T>> {
		const op = lift((zset: ZSet<T>) => ZSetOperators.filter(zset, predicate));
		return new Circuit(op);
	}

	static project<T, U>(projector: (t: T) => U): Circuit<ZSet<T>, ZSet<U>> {
		const op = lift((zset: ZSet<T>) => ZSetOperators.project(zset, projector));
		return new Circuit(op);
	}

	// ========== NON-LINEAR OPERATIONS (need incrementalize) ==========
	static distinct<T>(): Circuit<ZSet<T>, ZSet<T>> {
		const op = optimizedDistinctIncremental<T>(new ZSetGroup<T>());
		return new Circuit(op);
	}
	/*
	 * @PERF this
	 */
	static olddistinct<T>(): Circuit<ZSet<T>, ZSet<T>> {
		const op = incrementalize(
			lift((zset: ZSet<T>) => ZSetOperators.distinct(zset)),
			new ZSetGroup<T>(),
			new ZSetGroup<T>()
		);
		return new Circuit(op);
	}

	// ========== SET OPERATIONS (composite, need incrementalize) ==========

	static union<T>(): Circuit<[ZSet<T>, ZSet<T>], ZSet<T>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<T>]) => ZSetOperators.union(a, b)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<T>()),
			new ZSetGroup<T>()
		);
		return new Circuit(op);
	}

	static difference<T>(): Circuit<[ZSet<T>, ZSet<T>], ZSet<T>> {
		const op = incrementalize(
			lift(([a, b]: [ZSet<T>, ZSet<T>]) => ZSetOperators.difference(a, b)),
			new TupleGroup(new ZSetGroup<T>(), new ZSetGroup<T>()),
			new ZSetGroup<T>()
		);
		return new Circuit(op);
	}

	// ========== AGGREGATION OPERATIONS (when implemented) ==========

	// Note: These would need to be implemented once the ZSetOperators stubs are filled
	// static count<T>(): Circuit<ZSet<T>, number> { ... }
	// static sum<T>(): Circuit<ZSet<T>, number> { ... }
	// etc.
}
