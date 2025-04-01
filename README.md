# Simple SQLite ORM

A lightweight, type-safe SQLite ORM (Object-Relational Mapping) library for Python that provides a simple and intuitive interface for database operations.

## Features

- **Type Safety**: Full type hints support for better IDE integration and code clarity
- **Connection Pooling**: Efficient connection management with a configurable pool size
- **Transaction Support**: Atomic operations with automatic rollback on errors
- **Query Building**: Intuitive query building with support for complex conditions
- **Bulk Operations**: Efficient handling of multiple records
- **Security**: Built-in SQL injection protection and input validation
- **Indexing**: Support for creating and managing indexes
- **Foreign Keys**: Support for referential integrity
- **Data Export**: Export data to CSV and JSON formats

## Installation

```bash
pip install cs50  # Required dependency
```

## Quick Start

```python
from db_orm.db.models import Model, OR, LIKE, ORDER_BY

# Create a new table
users = Model("users")
users.addColumn("id", "int", primary_key=True)
users.addColumn("name", "str", max_length=100)
users.addColumn("email", "str", max_length=255)
users.addUniqueConstraint(["email"])
users.create()

# Insert a record
users.insert(name="John Doe", email="john@example.com")

# Query records
all_users = users.all()
john = users.filter(name="John Doe")
active_users = users.filter(LIKE(status="active"))

# Update a record
john[0].update(name="John Smith")

# Delete a record
john[0].delete()
```

## Design Principles

### 1. Simple and Intuitive API

The ORM follows a simple and intuitive API design that makes it easy to work with databases:

```python
# Create a table
model = Model("table_name")
model.addColumn("column_name", "type", **options)
model.create()

# Query data
results = model.filter(condition1="value1", condition2="value2")
```

### 2. Type Safety

The library uses Python's type hints to provide better IDE support and catch type-related errors early:

```python
def addColumn(self, name: str, type: str, **kwargs) -> None:
    # Type hints ensure correct usage
    pass
```

### 3. Connection Management

Efficient connection handling with a connection pool:

```python
@contextmanager
def get_connection(self):
    # Manages database connections efficiently
    pass
```

### 4. Transaction Support

Atomic operations with automatic rollback:

```python
with model.transaction() as conn:
    # All operations within this block are atomic
    pass
```

## Advanced Usage

### Complex Queries

```python
# OR conditions
results = users.filter(OR(status="active", status="pending"))

# LIKE queries
results = users.filter(LIKE(name="John%"))

# Ordering
results = users.filter(ORDER_BY("created_at", descending=True))

# Pagination
results = users.filter({"LIMIT": 10, "OFFSET": 20})
```

### Bulk Operations

```python
# Bulk insert
users.bulk_insert([
    {"name": "User 1", "email": "user1@example.com"},
    {"name": "User 2", "email": "user2@example.com"}
])

# Bulk update
users.bulk_update([
    {"id": 1, "name": "Updated User 1"},
    {"id": 2, "name": "Updated User 2"}
], key_column="id")

# Bulk delete
users.bulk_delete([1, 2], key_column="id")
```

### Indexes and Constraints

```python
# Create an index
users.create_index("idx_email", ["email"], unique=True)

# Add a foreign key
posts = Model("posts")
posts.addColumn("user_id", "int")
posts.addForeignKey("user_id", "users(id)", on_delete="CASCADE")

# Add a unique constraint
users.addUniqueConstraint(["email"])
```

### Data Export

```python
# Export to CSV
users.toCSV()

# Export to JSON
users.toJSON()
```

## Error Handling

The ORM provides clear error messages and custom exceptions:

```python
try:
    users.insert(name="John Doe", email="john@example.com")
except ValidationError as e:
    print(f"Validation error: {e}")
except DatabaseError as e:
    print(f"Database error: {e}")
```

## Best Practices

1. **Use Transactions**: Always use transactions for operations that modify multiple records:
   ```python
   with model.transaction() as conn:
       # Perform multiple operations
   ```

2. **Use Bulk Operations**: For multiple records, use bulk operations instead of individual operations:
   ```python
   model.bulk_insert(records)  # Instead of multiple insert() calls
   ```

3. **Create Indexes**: Create indexes for frequently queried columns:
   ```python
   model.create_index("idx_name", ["name"])
   ```

4. **Use Type Hints**: Leverage type hints for better code quality:
   ```python
   def filter(self, *args, **kwargs) -> List[QuerySet]:
       pass
   ```

5. **Handle Errors**: Always handle potential errors:
   ```python
   try:
       model.create()
   except DatabaseError as e:
       # Handle error appropriately
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.