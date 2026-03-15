import * as mysqlDriver from './mysql.js';
import * as postgresqlDriver from './postgresql.js';
import * as sqliteDriver from './sqlite.js';
import * as mongodbDriver from './mongodb.js';

const drivers = {
  mysql: mysqlDriver,
  postgresql: postgresqlDriver,
  sqlite: sqliteDriver,
  mongodb: mongodbDriver
};

export function getDriver(dbType) {
  const driver = drivers[String(dbType || 'mysql').toLowerCase()];
  if (!driver) {
    throw new Error(`不支持的数据库类型: ${dbType}，支持: ${Object.keys(drivers).join(', ')}`);
  }
  return driver;
}

export function getSupportedTypes() {
  return Object.keys(drivers);
}
