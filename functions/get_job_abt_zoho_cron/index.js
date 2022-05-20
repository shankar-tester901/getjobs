const catalyst = require("zcatalyst-sdk-node");
const axios = require('axios');
const cheerio = require("cheerio");

module.exports = (cronDetails, context) => {


    const catalystApp = catalyst.initialize(context);

    var title = 'Empty';
    var company = 'Empty';
    var location = 'Empty';
    var salary = 'Empty';
    var skill = 'Empty';

    const url = 'https://www.indeed.com/jobs?q=nasa&l=United+States';

    axios.get(url).then(response => {
        //	console.log('in get url axios ' + response.data);
        getData(response.data, context, catalystApp);
    }).catch(err => {
        console.log(err);
        context.closeWithFailure();
    })



    let getData = (page_html, context, catalystApp) => {
        //	console.log(html);
        data = [];
        //This parses the entire page in one go
        const $ = cheerio.load(page_html);
        //	console.log('page cheerio is ' + $);
        $('div.row.result').each((i, elem) => {
            //parsed and pushed all into the data array created above
            data.push({

                title: $(elem).find("a").attr("title"),
                company: $(elem).children().find("span.company").text(),
                location: $(elem).children().find("span.location").text(),
                salary: $(elem).children().find("span.salaryText").text(),
                skill: $(elem).children().find("div.summary ul li").text(),

            });
            //	console.log('Data to be inserted is ---------   ' + JSON.stringify(data));
        });

        dropAllRowsFromTable(catalystApp).then(() => {
            prepareForDBInsert(catalystApp, data, context);
        });


    }

}

function dropAllRowsFromTable(catalystApp) {


    return new Promise((resolve, reject) => {


        console.log('--------------- ABOUT TO DELETE ALL ROWS -------------');
        let datastore = catalystApp.datastore();
        let table = datastore.table('NasaJobs');
        let q_zcql = catalystApp.zcql();
        let zcqlPromise = q_zcql.executeZCQLQuery("select * from NasaJobs");
        zcqlPromise.then(queryResult => {
            if (queryResult.length > 0) {
                let promiseArr = [];
                for (i = 0; i < queryResult.length; i++) {
                    //		console.log('about to delete a row ');
                    let rowPromise = table.deleteRow(queryResult[i].NasaJobs.ROWID);
                    promiseArr.push(rowPromise);
                };

                // resolving all the delete promises
                Promise.all(promiseArr).then(results => {
                    //	console.log('Results' + results);
                    resolve();
                }).catch(errors => {
                    console.log('' + errors);
                    reject();
                })
            } else {
                console.log('No rows present in table');
                resolve();
            }
        })


    })


}

function prepareForDBInsert(catalystApp, jobEntries, context) {
    var rows = [];
    for (count = 0; count < jobEntries.length; count++) {

        var rowData = {
            title: jobEntries[count].title,
            company: jobEntries[count].company,
            location: jobEntries[count].location,
            salary: jobEntries[count].salary,
            skill: jobEntries[count].skill
        };
        //console.log('RowData is   ' + JSON.stringify(rowData));
        rows.push(rowData);

    }
    insert_To_DB(catalystApp, rows, context);

}

function insert_To_DB(catalystApp, rowData, context) {
    try {
        console.log('--------------- ABOUT TO INSERT ROWS -------------');

        let datastore = catalystApp.datastore();
        let table = datastore.table("NasaJobs");


        let insertPromise = table.insertRows(rowData);
        insertPromise.then(row => {
            //	console.log(row);
            console.log('inserted in db now ');
            context.closeWithSuccess();

        }).catch(err => {
            console.log('Error making db insert ' + err);
            context.closeWithFailure();
        });
    } catch {
        console.log('Issue in inserting. Please debug. ');
        context.closeWithFailure();

    }
};