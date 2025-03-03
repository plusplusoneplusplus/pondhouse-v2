# Query Module

This module contains the query processing components of the lakehouse-like system.

## Components

### Parser - already implemented as part of hyrise

### Planner

#### Logical planner

Logical planner is responsible for creating a logical plan for a given query. It's a high level abstraction of the query plan and decribes what operations are needed to execute the query. It's independent of the actual data and the storage engine. It takes an abstract syntax tree (AST) and converts it into a logical plan, which is a tree of logical plan nodes.

For a query like `SELECT name FROM users WHERE age > 30`, the logical plan might look like this:

```
LogicalPlan
 └── Projection: name
      └── Filter: age > 30
           └── Scan: users
```

Logical planner has following modules:

##### LogicalPlanNode

`LogicalPlanNode` is the base class for all logical plan nodes.

There are 4 types of logical plan nodes:
    - `Projection`: Represents a projection operation. Usually the root node of the logical plan.
    - `Scan`: Represents a scan operation on a table. It's the leaf node of the logical plan.
    - `Filter`: Represents a filter operation.
    - `Join`: Represents a join operation.

Please note, in this design, the `Scan` node is always the leaf node of the logical plan. If the `Scan` node doesn't have a filter condition as its parent, it means that the filter condition is not pushed down and will not be evaluated during the scan.

##### LogicalPlanOptimizer

`LogicalPlanOptimizer` is the base class for all logical plan optimizers.

Logical planner has the responsibility of optimizing the query plan. For example, it might decide to push down the filter condition to the scan operation if it can be evaluated before the scan.

For the same query, the logical plan might look like this after optimization:

```
LogicalPlan
 └── Projection: name
      └── Filter: age > 30 (pushed down)
           └── Scan: users (with column pruning: name, age)
```

Common optimizations:

- (Implemented) Push down filter conditions to the scan operation if possible
- (Implemented) Column pruning to reduce the number of columns read from the storage engine
- (Implemented) Push down projection to the scan operation if possible
- (Implemented) Constant folding
- Combine multiple scan operations into a single scan operation if possible
- Change the order of the joins based on the table cardinalities
- Transform the subqueries into joins if possible
- Replace complex expressions with simpler ones
- Optimize common table expressions (CTEs)

After optimization, a typical logical plan might look like this:

```
SELECT table1.name, table2.name
FROM table1
INNER JOIN table2 ON table1.id = table2.id
WHERE table1.age > 30 AND table2.age > 30;

LogicalPlan
 └── Projection: table1.name, table2.name
      └── Join: table1.id = table2.id
           └── Filter: table1.age > 30
                └── Scan: table1
           └── Filter: table2.age > 30
                └── Scan: table2
```

##### LogicalPlanVisitor

`LogicalPlanVisitor` is the base class for all logical plan visitors.

Logical planner visitor pattern is used to visit the logical plan nodes and perform the operations.
common operations:

- Plan virtualization: LogicalPlanPrinter is used to print the logical plan in a user friendly format.
- Cost estimation
- Plan validation
- Physical plan generation
- Statistics collection

#### Physical planner

Physical planner is a lower level, concrete plan that describes how the operations are executed. It's dependent on the actual data and the storage engine. It will include the details like the actual algorithms used for the operations, for example, hash join vs sort merge join and the data access methods, for example, sequential scan vs index scan.

Physical planner converts the logical plan into a physical plan, and use the cost-based optimizer and rule-based optimizer to optimize the physical plan.

For the same query as above, the physical plan might look like this:

```
PhysicalPlan
└── Sequential Scan: users (with column pruning: name, age) & Projection: name & Filter: age > 30 // Projection and filter will be evaluated during the scan
```

or if there is an index on the age column, it might look like this:

```
PhysicalPlan
└── Index Scan: users (with column pruning: name, age) & Projection: name & Filter: age > 30 // Projection and filter will be evaluated during the scan
```

##### PhysicalPlanNode

`PhysicalPlanNode` is the base class for all physical plan nodes.

There are 4 types of physical plan nodes:
    - `Projection`: Represents a projection operation.
    - `Filter`: Represents a filter operation.
    - `Join`
        - `HashJoin`: Represents a hash join operation.
        - `SortMergeJoin`: Represents a sort merge join operation.
        - `NestedLoopJoin`: Represents a nested loop join operation.
    - `Scan`
        - `SequentialScan`: Represents a sequential scan operation on a table.
            - May have Projection and Filter as its children
        - `IndexScan`: Represents an index scan operation on a table.
            - May have Projection and Filter as its children
    - `Exchange`
        - `ShuffleExchange`: Represents a shuffle/exchange operation for data redistribution.
        - `BroadcastExchange`: Represents a broadcast/exchange operation for data redistribution.

##### PhysicalPlanOptimizer

`PhysicalPlanOptimizer` is the base class for all physical plan optimizers.

Physical planner also has the responsibility of optimizing the query plan. For example, it might decide to use a hash join instead of a sort merge join if the table is large enough and the join condition is a hashable column.

Common optimizations:

- Choose specific algorithms for the operations based on the data and the storage engine. E.g. nested loop join vs hash join vs sort merge join
- Choose specific data access methods based on the data and the storage engine. E.g. sequential scan vs index scan
- Skip the partitions of the data that is irrelevant to the query
- Cache the intermediate results of the operations if possible
- Combine operations (e.g. filter and projection) into a single operation to reduce the number of passes over the data
- For OLAP workload, shuffle/exchange the data to redistribute the data to the nodes in a more balanced way (avoiding hotspots) or colocating the data to the nodes.
    - Example: 
    ```
    SELECT department, SUM(salary)
    FROM employees
    GROUP BY department;
    ```
    
    Physical planner might generate a plan like this:

    ```
    PhysicalPlan
    └── ShuffleExchange: HashPartitioning(department)
        └── PartialAggregate: SUM(salary), department
            └── Scan: employees
    ```
    This will redistribute rows by the department column to ensure all rows for the same department are on the same node.

- Cost Model: Cost model is used to estimate the cost of the physical plan. It's used by the cost-based optimizer to choose the optimal physical plan.
- Rule-based optimizer: Rule-based optimizer is used to optimize the physical plan. It's a set of rules that are applied to the physical plan to transform it into an optimal physical plan.


### Executor

The executor is responsible for executing the physical plan.

It has 2 major components:
- Coordinator node: responsible for distributing the physical plan to the worker nodes and collecting the results.
- Worker nodes: responsible for executing the physical plan.

On the work nodes, the Execution framework consists of:
- Context for the runtime state
- Memory management
- Expression evaluation engine
- Statistics collection

Operator implementation
- Iterator mode (pull-based) v.s. volcano mode v.s. push-based mode
    - The hybrid mode is picked. Inner node executor is using volcano mode, while with exchange node, it's using push-based mode.
- Vectorized execution
- Spilling to disk if the memory is insufficient


### Data Access Layer

┌──────────────┐       ┌─────────────┐       ┌───────────────┐
│   Executor   │──────>│ DataAccessor│<──────│ ParquetReader │
└──────┬───────┘       └──────┬──────┘       └───────────────┘
       │                      │
       │                ┌─────▼─────┐
       │                │  Catalog  │
       └───────────────>└───────────┘

Executor          PhysicalScanNode     Catalog       DataAccessor     ParquetReader
   │───────────────────>│                │               │                 │
   │                    │──GetTableName─>│               │                 │
   │                    │<───Metadata────│               │                 │
   │─────────────────────────────────────>GetDataFiles──>│                 │
   │                    │                │               │──CreateReader───>│
   │<───RecordBatches────────────────────────────────────│<────Read───────│