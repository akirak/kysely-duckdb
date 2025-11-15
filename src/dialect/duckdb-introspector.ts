import type {
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  Kysely,
  SchemaMetadata,
  TableMetadata,
} from 'kysely'

/**
 * DuckDB database introspector
 */
export class DuckDbIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<any>

  constructor(db: Kysely<any>) {
    this.#db = db
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    const result = await this.#db
      .selectFrom('information_schema.schemata' as any)
      .select(['schema_name as name'])
      .execute()

    return result.map(row => ({
      name: row.name as string,
    }))
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    let query = this.#db
      .selectFrom('information_schema.tables as tables' as any)
      .leftJoin('information_schema.columns as columns' as any, join =>
        join
          .onRef('columns.table_schema', '=', 'tables.table_schema')
          .onRef('columns.table_name', '=', 'tables.table_name'),
      ) as any

    query = query
      .select([
        'tables.table_name as name',
        'tables.table_schema as schema',
        'tables.table_type as type',
        'columns.column_name as columnName',
        'columns.data_type as columnDataType',
        'columns.is_nullable as columnIsNullable',
        'columns.column_default as columnDefault',
        'columns.character_maximum_length as columnMaxLength',
        'columns.numeric_precision as columnPrecision',
        'columns.numeric_scale as columnScale',
      ] as any)
      .where('tables.table_schema', '!=', 'information_schema')
      .where('tables.table_schema', '!=', 'pg_catalog')
      .orderBy('tables.table_schema')
      .orderBy('tables.table_name')
      .orderBy('columns.ordinal_position')

    if (options.withInternalKyselyTables !== true) {
      query = query
        .where('tables.table_name', '!=', 'kysely_migration')
        .where('tables.table_name', '!=', 'kysely_migration_lock')
    }

    const result = await query.execute()

    const tablesMap = new Map<string, TableMetadata>()

    for (const row of result) {
      const schema = row.schema as string
      const name = row.name as string
      const key = `${schema}.${name}`

      let table = tablesMap.get(key)
      if (!table) {
        table = {
          name,
          schema,
          columns: [],
          isView: (row.type as string).toLowerCase() === 'view',
        }
        tablesMap.set(key, table)
      }

      if (row.columnName) {
        table.columns.push({
          name: row.columnName as string,
          dataType: row.columnDataType as string,
          isNullable: (row.columnIsNullable as string) === 'YES',
          hasDefaultValue: row.columnDefault !== null,
          isAutoIncrementing: false,
        })
      }
    }

    return Array.from(tablesMap.values())
  }

  async getMetadata(options?: DatabaseMetadataOptions): Promise<DatabaseMetadata> {
    const tables = await this.getTables(options)

    return {
      tables,
    }
  }

}
