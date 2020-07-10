const cluster = require('cluster');

if (cluster.isMaster) {

    // Count the machine's CPUs
    var cpuCount = 5;

    // Create a worker for each CPU
    for (var i = 0; i < cpuCount; i += 1) {
        cluster.fork();
    }
   
} else {
    const express = require("express");
    const app = express();
    const bodyParser = require('body-parser')
    const cors = require('cors')
    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
      contactPoints: ['localhost'],
      localDataCenter: 'datacenter1',
      keyspace: 'test_keyspace'
    });
     
    app.use(cors());
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: false }))

    var server = app.listen(9009, () => {
        console.log("Server running on port 9009");
    });
    server.on('connection', function (socket) {
        console.log("New incoming connection")
        socket.setTimeout(200 * 1000);
    });

    app.get("/v1/readUser/:id", (req, res, next) => {
        const query = 'SELECT * FROM test_keyspace.user WHERE id = ?';
        let id = req.params.id;

        // Set the prepare flag in your queryOptions
        client.execute(query, [id], { prepare: true }, function (err, results) {                     
            if (err) {
                console.log(err)
                throw err
            }
            var user = results.first();
            if (user)
                res.status(200).send(`User retrieved : ${user.id} ${user.userid} ${user.username}`)
            else 
                res.status(404).send('NotFound')
        })
    });

    app.post('/v1/addUser', function (req, res) {
        let data = req.body
        const query = 'INSERT INTO test_keyspace.user(id,channel,countrycode,createdby,createddate,currentlogintime,dob,email,emailverified,firstname,flagsvalue,framework,gender,grade,language,lastname,loginid,password,phone,phoneverified,profilevisibility,status,updatedby,updateddate,userid,username,usertype) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

        const params = [data.id, "testchannel", "+91", data.createdby, data.createddate, data.currentlogintime,
        data.dob, data.email, true, data.firstname, data.flagsvalue, data.framework, data.gender, data.grade, data.language, data.lastname,
        data.loginid, "password", data.phone, true, data.profilevisibility, data.status, data.updatedby, data.updateddate, data.userid, data.username, data.usertype];
        client.execute(query, params, { prepare: true }, function (err) {
            if (err) {
                console.log(err)
                throw err
            }
            res.status(200).json({user_id : data.id});
        });
    })

    app.post('/v1/addReadUser', function (req, res) {
        let data = req.body
        const query = 'INSERT INTO test_keyspace.user(id,channel,countrycode,createdby,createddate,currentlogintime,dob,email,emailverified,firstname,flagsvalue,framework,gender,grade,language,lastname,loginid,password,phone,phoneverified,profilevisibility,status,updatedby,updateddate,userid,username,usertype) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

        const params = [data.id, "testchannel", "+91", data.createdby, data.createddate, data.currentlogintime,
        data.dob, data.email, true, data.firstname, data.flagsvalue, data.framework, data.gender, data.grade, data.language, data.lastname,
        data.loginid, "password", data.phone, true, data.profilevisibility, data.status, data.updatedby, data.updateddate, data.userid, data.username, data.usertype];
        client.execute(query, params, { prepare: true }, function (err) {
            if (err) {
                console.log(err)
                throw err
            }
            const readQuery = 'SELECT * FROM test_keyspace.user WHERE id = ?';
            client.execute(readQuery, [data.id], { prepare: true }, function (err, results) {                     
                if (err) {
                    console.log(err)
                    throw err
                }
                var user = results.first();
                if (user)
                    res.status(200).send(`User retrieved : ${user.id} ${user.userid} ${user.username}`);
                else 
                    res.status(404).send('NotFound');
            });
        })
    })
} 