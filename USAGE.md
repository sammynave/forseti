# Table of Contents

## [High-Level API (Recommended for Most Users)](#high-level-api-recommended-for-most-users)
- [ReactiveTable](#reactivetable---crud-operations-with-automatic-change-notifications)
- [QueryBuilder](#querybuilder---fluent-api-for-reactive-queries)
- [Joins](#joins---combine-multiple-tables-reactively)

## [Circuit Operations](#circuit-operations---low-level-incremental-processing)
- [Linear Operations](#linear-operations-automatically-incremental)
- [Bilinear Operations](#bilinear-operations-optimized-for-incremental-processing)
- [Set Operations](#set-operations)
- [Ordering Operations](#ordering-operations)
- [Circuit Composition](#circuit-composition)

## [ZSet Operations](#zset-operations---core-data-structure-operations)
- [Basic ZSet Operations](#basic-zset-operations)

## [Stateful Components](#stateful-components---true-incremental-processing)
- [StatefulJoin](#statefujoin---oδ-incremental-joins)
- [StatefulTopK](#stateful-top-k---incremental-top-k-maintenance)
- [StatefulJoinCircuit](#statefuljoincircuit---complete-join-circuit-with-state)

## [Stream Processing](#stream-processing---time-based-incremental-processing)
- [Stream Operations](#stream-operations)

## [Key Features](#key-features)

## [Performance Characteristics](#performance-characteristics)

---

## __High-Level API (Recommended for Most Users)__

### __ReactiveTable__ - CRUD operations with automatic change notifications

```typescript
import { createReactiveTable } from './reactive-table.js';

const users = createReactiveTable([
  { id: 1, name: "Alice", age: 25 },
  { id: 2, name: "Bob", age: 30 }
], 'id');

// CRUD operations
users.add({ id: 3, name: "Charlie", age: 28 });
users.update(1, { age: 26 });
users.remove(2);

// Subscribe to changes
users.subscribe((delta) => console.log('Changes:', delta));
```

### __QueryBuilder__ - Fluent API for reactive queries

```typescript
import { createQuery } from './query-builder.js';

const query = createQuery(users)
  .where(user => user.age > 25)
  .sortBy('name', 'asc')
  .limit(10, 0);

// Get reactive store (Svelte-compatible)
const reactiveResult = query.reactive();
reactiveResult.subscribe(results => console.log(results));

// Or get snapshot
const snapshot = query.toArray();
```

### __Joins__ - Combine multiple tables reactively

```typescript
const orders = createReactiveTable([
  { id: 1, userId: 1, amount: 100 },
  { id: 2, userId: 2, amount: 200 }
], 'id');

const joinedQuery = createQuery(users)
  .join(orders, user => user.id, order => order.userId)
  .select((user, order) => ({ userName: user.name, orderAmount: order.amount }))
  .where((user, order) => order.amount > 150);
```

## __Circuit Operations__ - Low-level incremental processing

### __Linear Operations__ (automatically incremental)

```typescript
import { Circuit } from './circuit.js';

// Filter
const filterCircuit = Circuit.filter<User>(user => user.age > 18);

// Project/Transform
const projectCircuit = Circuit.project<User, string>(user => user.name);
```

### __Bilinear Operations__ (optimized for incremental processing)

```typescript
// Equi-join
const equiJoinCircuit = Circuit.equiJoin<User, Order, number>(
  user => user.id,
  order => order.userId
);

// Cartesian product
const cartesianCircuit = Circuit.cartesianProduct<User, Role>();

// General join with predicate
const joinCircuit = Circuit.join<User, Order>(
  (user, order) => user.id === order.userId && order.amount > 100
);

// Set intersection
const intersectCircuit = Circuit.intersect<User>();
```

### __Set Operations__

```typescript
// Union
const unionCircuit = Circuit.union<User>();

// Difference
const differenceCircuit = Circuit.difference<User>();

// Distinct
const distinctCircuit = Circuit.distinct<User>();
```

### __Ordering Operations__

```typescript
// Top-K with limit and offset
const topKCircuit = Circuit.topK<User>(
  (a, b) => a.age - b.age,
  { limit: 10, offset: 0 }
);

// Order by field
const orderByCircuit = Circuit.orderBy<User, number>(
  user => user.age,
  (a, b) => a - b,
  { limit: 5 }
);
```

### __Circuit Composition__

```typescript
// Chain operations
const composedCircuit = Circuit
  .filter<User>(user => user.age > 18)
  .compose(Circuit.project(user => user.name))
  .compose(Circuit.distinct());

// Execute circuit
const input = new Stream(new ZSet([/* data */]));
const output = composedCircuit.execute(input);
```

## __ZSet Operations__ - Core data structure operations

### __Basic ZSet Operations__

```typescript
import { ZSet } from './z-set.js';
import { ZSetOperators } from './z-set-operators.js';

const users = new ZSet([
  [{ id: 1, name: "Alice" }, 1],
  [{ id: 2, name: "Bob" }, 1]
]);

// Filter
const adults = ZSetOperators.filter(users, user => user.age >= 18);

// Project
const names = ZSetOperators.project(users, user => user.name);

// Joins
const joined = ZSetOperators.equiJoin(
  users, orders,
  user => user.id,
  order => order.userId
);

// Set operations
const union = ZSetOperators.union(setA, setB);
const difference = ZSetOperators.difference(setA, setB);
const intersection = ZSetOperators.intersect(setA, setB);
```

## __Stateful Components__ - True incremental processing

### __StatefulJoin__ - O(|Δ|) incremental joins

```typescript
import { StatefulEquiJoin } from './stateful-join.js';

const statefulJoin = new StatefulEquiJoin(
  user => user.id,
  order => order.userId,
  new ZSetGroup()
);

// Process only deltas, not full datasets
const joinDelta = statefulJoin.processIncrement(userDelta, orderDelta);
const fullJoinResult = statefulJoin.getMaterializedView();
```

### __StatefulTopK__ - Incremental top-K maintenance

```typescript
import { StatefulTopK } from './stateful-top-k.js';

const topK = new StatefulTopK(
  (a, b) => a.score - b.score,
  10, // limit
  0,  // offset
  new ZSetGroup()
);

const topKDelta = topK.processIncrement(newDataDelta);
```

### __StatefulJoinCircuit__ - Complete join circuit with state

```typescript
import { createStatefulJoinCircuit } from './stateful-circuit.js';

const joinCircuit = createStatefulJoinCircuit(
  user => user.id,
  order => order.userId
);

// Initialize with base data
joinCircuit.initialize(initialUsers, initialOrders);

// Process incremental updates
const joinDelta = joinCircuit.processIncrement(userDeltas, orderDeltas);
```

## __Stream Processing__ - Time-based incremental processing

### __Stream Operations__

```typescript
import { Stream, incrementalize } from './stream.js';
import { ZSetGroup } from './z-set.js';

// Create streams
const userStream = new Stream(new ZSet([]));
userStream.set(0, userDelta1);
userStream.set(1, userDelta2);

// Incrementalize operators
const incrementalFilter = incrementalize(
  lift(users => ZSetOperators.filter(users, u => u.age > 18)),
  new ZSetGroup(),
  new ZSetGroup()
);

const outputStream = incrementalFilter(userStream);
```

## __Key Features__

1. __Incremental Processing__: All operations process only changes (deltas), not full datasets
2. __Reactive__: Automatic propagation of changes through query graphs
3. __Composable__: Operations can be chained and combined
4. __Optimized__: Special optimizations for bilinear operations (joins)
5. __Type Safe__: Full TypeScript support with generic types
6. __DBSP-based__: Implements formal incremental computation theory
7. __Memory Efficient__: Maintains only necessary state for incremental processing
8. __Svelte Compatible__: Reactive queries work as Svelte stores

## __Performance Characteristics__

- __Linear operations__: O(|Δ|) - only process changes
- __Bilinear operations__ (joins): O(|Δ|) with persistent indexes
- __Non-linear operations__ (distinct, topK): Custom incremental algorithms
- __Memory usage__: O(|active_state|) instead of O(|full_history|)

This library provides both high-level convenient APIs and low-level control for building efficient reactive data processing pipelines.
