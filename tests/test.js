//@ts-check
import { adapter, createMsSqlPool } from '../src/index.js';
import { testAdapter } from '@kinshipjs/adapter-tests';

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
    database: "chinook_ks_test",
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

await testAdapter(connection, {
    albumsTableName: "dbo.Album",
    genresTableName: "dbo.Genre",
    playlistsTableName: "dbo.Playlist",
    playlistTracksTableName: "dbo.PlaylistTrack",
    tracksTableName: "dbo.Track",
    precision: 4
});
process.exit(1);