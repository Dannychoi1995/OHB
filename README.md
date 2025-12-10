# 🍸 One Handed Bartender - Business Tracker

A comprehensive business management system for tracking distillery operations, inventory, orders, and financial reports.

## Features

- 📊 **Dashboard**: Overview of key metrics and recent activity
- 🍾 **Finished Goods**: Track bottled products and inventory
- 🥃 **Bulk Spirits**: Manage bulk spirit inventory and aging
- 🔄 **Batches**: Track production batches and their lifecycle
- 📦 **Inventory Tracking**: Monitor raw materials and supplies
- 💰 **Purchase Orders**: Create and manage supplier orders
- 📋 **Recipes**: Define and manage product recipes
- ⚙️ **Production**: Record production runs and track materials used
- 📄 **Invoices**: Generate invoices for customers
- 🎁 **Samples**: Track sample distribution
- 🔍 **Physical Counts & Waste**: Record inventory counts and waste
- 📈 **Reports & Analytics**: Comprehensive business analytics
- 💵 **Financial Reports**: Track revenue, costs, and profitability
- 💼 **CRM/Sales**: Manage customers and sales orders

## Installation

```bash
pip install -r requirements.txt
```

## Running Locally

```bash
streamlit run app.py
```

## Deployment

This app is designed to be deployed on [Streamlit Cloud](https://streamlit.io/cloud).

### Note on Database
The app uses SQLite for data storage. On Streamlit Cloud, the database will reset on each deployment. For production use, consider integrating with a persistent database solution like PostgreSQL.

## Tech Stack

- **Streamlit**: Web application framework
- **SQLite**: Local database (via sqlite-utils)
- **Pandas**: Data manipulation and analysis
- **Python 3.14+**: Core language

## License

All rights reserved - One Handed Bartender

