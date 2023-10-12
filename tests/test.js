//@ts-check
import { KinshipContext } from '@kinshipjs/core';
import { adapter, createMsSqlPool } from '../src/index.js';

const pool = createMsSqlPool({
    database: "kinship_test",
    server: "192.168.1.28",
    user: "sa",
    password: "mySuperSecretPassw0rd!",
    port: 15301,
});

const connection = adapter(pool);
/** @type {KinshipContext<import('../../core/test/test.js').User>} */
const ctx = new KinshipContext(connection, "Auth.User");
