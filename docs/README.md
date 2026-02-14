## Documentation

Architecture and data model diagrams for the project.

## Data Model Overview

The analytical model is implemented in the Gold layer using a star schema
design. Conformed dimensions are shared across multiple fact tables to
support cross-domain analysis.

Due to the size of the model, it is documented using multiple screenshots
for clarity.

### Dimensions
The following dimensions are used across fact tables:
- Date
- Customers
- Products
- Sellers
- Customer Orders

(See: data_model_dimensions.png)

### Fact Tables
The model includes separate fact tables to represent different business
processes:
- Sales
- Orders
- Payments
- Shipping
- Reviews

Each fact table is linked to relevant dimensions at the appropriate grain.
(See: data_model_facts.png)
