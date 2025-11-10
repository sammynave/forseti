import { ZSet } from './z-set.js';

/*
// Cartesian product
const users = new ZSet([["alice", 1], ["bob", 1]]);
const roles = new ZSet([["admin", 1], ["user", 1]]);
const userRoles = ZSetOperators.cartesianProduct(users, roles);
// Result: [["alice", "admin"], 1], [["alice", "user"], 1], [["bob", "admin"], 1], [["bob", "user"], 1]

// Equi-join
const orders = new ZSet([[{id: 1, userId: "alice"}, 1], [{id: 2, userId: "bob"}, 1]]);
const customers = new ZSet([[{id: "alice", name: "Alice"}, 1], [{id: "bob", name: "Bob"}, 1]]);
const joined = ZSetOperators.equiJoin(
  orders,
  customers,
  order => order.userId,
  customer => customer.id
);
*/

export class ZSetOperators {
	// ========== LINEAR AGGREGATION (Automatically Incremental) ==========
	// Paper Section 7.2: "The aggregation functions a_COUNT and a_SUM are in fact
	// linear transformations between the group Z[A] and the result group"

	/**
	 * COUNT: a_COUNT(s) = Σ_{x∈s} s[x] (sum of all weights)
	 * Linear operator - automatically incremental per Theorem 3.3
	 */
	static count<T>(zset: ZSet<T>): number {
		return zset.data.reduce((sum, [_, weight]) => sum + weight, 0);
	}

	/**
	 * SUM: a_SUM(s) = Σ_{x∈s} x × s[x] (weighted sum of values)
	 * Linear operator - automatically incremental per Theorem 3.3
	 */
	static sum<T>(zset: ZSet<T>, extractor: (record: T) => number): number {
		return zset.data.reduce((sum, [record, weight]) => {
			const value = extractor(record);
			return sum + value * weight;
		}, 0);
	}

	/**
	 * AVERAGE: Composite of SUM and COUNT
	 * Paper approach: Use makeset to convert back to Z-set for composition
	 */
	static average<T>(zset: ZSet<T>, extractor: (record: T) => number): number | null {
		const totalSum = this.sum(zset, extractor);
		const totalCount = this.count(zset);

		return totalCount === 0 ? null : totalSum / totalCount;
	}

	/**
	 * makeset: Convert scalar to singleton Z-set (from paper Section 7.2)
	 * makeset(x) = {x ↦ 1}
	 */
	static makeset<T>(value: T): ZSet<T> {
		return new ZSet([[value, 1]]);
	}

	// ========== GROUP-BY OPERATIONS ==========
	// Paper Section 7.3: "indexed Z-sets" - K → Z[A] = Z[A][K]

	/**
	 * GroupBy: G_p(a)[k] = Σ_{x∈a, p(x)=k} a[x] · x
	 *
	 * Paper Citation: Section 7.3 - "We define the grouping function G_p : Z[A] → Z[A][K]"
	 * Properties: "The grouping function G_p is linear for any key function p!"
	 * This means groupBy is automatically incremental.
	 */
	static groupBy<T, K>(zset: ZSet<T>, keyExtractor: (record: T) => K): Map<K, ZSet<T>> {
		const groups = new Map<K, ZSet<T>>();

		for (const [record, weight] of zset.data) {
			const key = keyExtractor(record);

			if (!groups.has(key)) {
				groups.set(key, new ZSet<T>([]));
			}

			groups.get(key)!.append([record, weight]);
		}

		// Merge records in each group
		for (const [key, group] of groups.entries()) {
			groups.set(key, group.mergeRecords());
		}

		return groups;
	}

	// ========== LINEAR OPERATIONS ==========
	// Paper Section 4.2, Table 1

	/**
	 * Filtering/Selection: σ_P(m)[x] = m[x] if P(x), 0 otherwise
	 *
	 * Paper Citation: Table 1 - "Filtering"
	 * SQL: "SELECT * FROM I WHERE p(I.c)"
	 * Properties: "σ_P is linear; ispositive(σ_P)"
	 */
	static filter<T>(zset: ZSet<T>, predicate: (record: T) => boolean): ZSet<T> {
		return new ZSet(zset.data.filter(([record]) => predicate(record)));
	}

	/**
	 * Projection: π(i)[y] = Σ_{x∈i, x|c=y} i[x]
	 *
	 * Paper Citation: Table 1 - "Projection"
	 * SQL: "SELECT DISTINCT I.c FROM I"
	 * Properties: "π is linear; ispositive(π)"
	 * Note: "x|c is projection on column c of the tuple x"
	 */
	static project<T, U>(zset: ZSet<T>, projector: (record: T) => U): ZSet<U> {
		const result = new ZSet<U>([]);
		for (const [record, weight] of zset.data) {
			const projected = projector(record);
			result.append([projected, weight]);
		}
		return result.mergeRecords();
	}

	/**
	 * Distinct: distinct(m)[x] = 1 if m[x] > 0, else 0
	 * Converts Z-set to set by removing duplicates and negative weights
	 */
	/**
	 * Distinct: distinct(m)[x] = 1 if m[x] > 0, 0 otherwise
	 *
	 * Paper Citation: Table 1 - "Union", "Difference"
	 * Also Definition 4.3 and surrounding text
	 * Note: "distinct eliminates duplicates. An implementation of UNION ALL does not need the distinct."
	 * Note: "distinct 'removes' duplicates from multisets, and it also eliminates elements with negative weights"
	 */
	static distinct<T>(zset: ZSet<T>): ZSet<T> {
		const map = zset.mergeRecords().data.reduce((acc, [r, w]) => {
			if (w < 1) return acc;
			if (acc.has(r)) return acc;
			acc.set(r, 1);
			return acc;
		}, new Map());
		return new ZSet(Array.from(map.entries()));
	}

	// ========== JOIN OPERATIONS ==========
	// Paper Section 4.2, Table 1

	/**
	 * Cartesian Product: (a × b)((x,y)) = a[x] × b[y]
	 *
	 * Paper Citation: Table 1 - "Cartesian product"
	 * SQL: "SELECT I1.*, I2.* FROM I1, I2"
	 * Properties: "× is bilinear, ispositive(×)"
	 */
	static cartesianProduct<T, U>(a: ZSet<T>, b: ZSet<U>): ZSet<[T, U]> {
		const result = new ZSet<[T, U]>([]);

		for (const [recordA, weightA] of a.data) {
			for (const [recordB, weightB] of b.data) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([[recordA, recordB], combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * Equi-join: (a ⊲⊳ b)((x,y)) = a[x] × b[y] if x|c1 = y|c2
	 *
	 * Paper Citation: Table 1 - "Equi-join"
	 * SQL: "SELECT I1.*, I2.* FROM I1 JOIN I2 ON I1.c1 = I2.c2"
	 * Properties: "⊲⊳ is bilinear, ispositive(⊲⊳)"
	 *
	 * @QUESTION does it make sense to build an index on every call?
	 */
	static equiJoin<T, U, K>(
		a: ZSet<T>,
		b: ZSet<U>,
		keyExtractorA: (record: T) => K,
		keyExtractorB: (record: U) => K
	): ZSet<[T, U]> {
		const result = new ZSet<[T, U]>([]);

		// Build index for b to make join more efficient
		const bIndex = new Map<string, Array<[U, number]>>();
		for (const [recordB, weightB] of b.data) {
			const keyB = keyExtractorB(recordB);
			const keyStr = JSON.stringify(keyB);
			if (!bIndex.has(keyStr)) {
				bIndex.set(keyStr, []);
			}
			bIndex.get(keyStr)!.push([recordB, weightB]);
		}

		// Join with indexed lookup
		for (const [recordA, weightA] of a.data) {
			const keyA = keyExtractorA(recordA);
			const keyStr = JSON.stringify(keyA);
			const matchingBRecords = bIndex.get(keyStr) || [];

			for (const [recordB, weightB] of matchingBRecords) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([[recordA, recordB], combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * Intersection: Special case of equi-join when both relations have same schema
	 *
	 * Paper Citation: Table 1 - "Intersection"
	 * SQL: "(SELECT * FROM I1) INTERSECT (SELECT * FROM I2)"
	 * Note: "Special case of equi-join when both relations have the same schema"
	 */
	static intersect<T>(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		const result = new ZSet<T>([]);

		// Build index for b
		const bIndex = new Map<string, number>();
		for (const [recordB, weightB] of b.data) {
			const keyStr = JSON.stringify(recordB);
			bIndex.set(keyStr, (bIndex.get(keyStr) || 0) + weightB);
		}

		// Find intersecting records
		for (const [recordA, weightA] of a.data) {
			const keyStr = JSON.stringify(recordA);
			const weightB = bIndex.get(keyStr);
			if (weightB !== undefined) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([recordA, combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * General Join: (a ⊲⊳ b)((x,y)) = a[x] × b[y] if predicate(x,y)
	 *
	 * Not explicitly in the paper, but follows the same bilinear pattern
	 * as other joins. Useful for implementing complex join conditions.
	 *
	 */
	static join<T, U>(
		a: ZSet<T>,
		b: ZSet<U>,
		predicate: (recordA: T, recordB: U) => boolean
	): ZSet<[T, U]> {
		const result = new ZSet<[T, U]>([]);

		for (const [recordA, weightA] of a.data) {
			for (const [recordB, weightB] of b.data) {
				if (predicate(recordA, recordB)) {
					const combinedWeight = weightA * weightB;
					if (combinedWeight !== 0) {
						result.append([[recordA, recordB], combinedWeight]);
					}
				}
			}
		}

		return result.mergeRecords();
	}

	// ========== SET OPERATIONS ==========
	// Paper Section 4.2, Table 1

	/**
	 * Union: I1 ∪ I2 = distinct(I1 + I2)
	 *
	 * Paper Citation: Table 1 - "Union"
	 * SQL: "(SELECT * FROM I1) UNION (SELECT * FROM I2)"
	 * Implementation: "I1 + I2 → distinct → O"
	 */
	static union<T>(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		// Use ZSetGroup.add() then distinct
		const sum = a.concat(b).mergeRecords(); // This is equivalent to group addition
		return this.distinct(sum);
	}

	/**
	 * Difference: I1 \ I2 = distinct(I1 - I2)
	 *
	 * Paper Citation: Table 1 - "Difference"
	 * SQL: "SELECT * FROM I1 EXCEPT SELECT * FROM I2"
	 * Implementation: "I1 - I2 → distinct → O"
	 * Note: "distinct removes elements with negative weights from the result"
	 */
	static difference<T>(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		// Subtract b from a, then apply distinct
		const negatedB = new ZSet(b.data.map(([r, w]) => [r, -w]));
		const diff = a.concat(negatedB).mergeRecords();
		return this.distinct(diff);
	}

	// ========== ORDERING OPERATIONS ==========

	/**
	 * TopK: Select top K elements by comparator
	 * Non-linear operator - needs custom incremental implementation
	 */
	static topK<T>(
		zset: ZSet<T>,
		comparator: (a: T, b: T) => number,
		k: number = Infinity,
		offset: number = 0
	): ZSet<T> {
		// Batch implementation for reference
		const sorted = [...zset.data]
			.filter(([_, weight]) => weight > 0)
			.sort(([a], [b]) => comparator(a, b));

		const topK = sorted
			.slice(offset, offset + k)
			.map(([record, weight]) => [record, Math.min(weight, 1)] as [T, number]);

		return new ZSet(topK);
	}

	// ============ window functions ==============
	/*
		ROW_NUMBER()
		RANK()
		LAG()/LEAD()
	*/
}
