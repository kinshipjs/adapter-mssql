//@ts-check
import { KinshipContext } from '@kinshipjs/core';
import { adapter, createMySql2Pool } from '../src/index.js';

const pool = createMySql2Pool({
    database: "kinship_test",
    host: "192.168.1.28",
    user: "root",
    password: "root",
    port: 10500
});

const connection = adapter(pool);
/** @type {KinshipContext<import('../../core/test/test.js').User>} */
const ctx = new KinshipContext(connection, "User");
ctx.where(m => m.Id.equals(1));
