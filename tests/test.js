//@ts-check
import { KinshipContext, transaction } from '@kinshipjs/core';
import { adapter, createMsSqlPool } from '../src/index.js';

/**
 * @typedef {object} User
 * @prop {number=} Id
 * @prop {string=} FirstName
 * @prop {string} LastName
 * @prop {number=} Age
 * 
 * @prop {Role[]=} Roles
 */

/**
 * @typedef {object} Role
 * @prop {number=} Id
 * @prop {string=} Title
 * @prop {string=} Description
 * @prop {number=} UserId
 */

const pool = await createMsSqlPool({
    database: "kinship_test",
    server: "192.168.1.28",
    user: "sa",
    password: "mySuperSecretPassw0rd!",
    port: 15301,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
});

const connection = adapter(pool);
/** @type {KinshipContext<User>} */
const ctx = new KinshipContext(connection, "Auth.User");

await transaction(ctx).execute(async ctx => {
    await ctx;
});