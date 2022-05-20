module.exports = (event, context) => {

	console.log(' ----------------- in EVENT LISTENER NOW  ---------------------- ');
	const catalyst = require('zcatalyst-sdk-node');
	const catalystApp = catalyst.initialize(context);
	console.log('Length is >>>>>>>>>>>>>>>>         ' + event.data.length);

	for (j = 0; j < event.data.length; j++) {
		//	console.log(' j is   ' + j);
		var event_info = event.data[j];
		//	console.log('Incoming event is ' + JSON.stringify(event_info));
		var location_info = event.data[j].location;
		console.log('LOCATION *********     ' + JSON.stringify(location_info));


		//if  location is GreenBelt, then mail
		if (location_info.indexOf("Greenbelt") !== -1) {

			//console.log("------------- ABOUT TO MAIL -------------------------   company : " + JSON.stringify(event_info.company) + "\n   title : " + JSON.stringify(event_info.title) + "\n   Salary Range : " + JSON.stringify(event_info.salary) + "\n   Skill :  " + JSON.stringify(event_info.skill));
			let config = {
				from_email: 's1002@gmail.com',
				to_email: 'sr1002@gmail.com',
				subject: 'Nasa Job Opportunity',
				content: "Nasa Opportunity Details Are As Follows ----\n \n" + "    Company :  " + JSON.stringify(event_info.company) + "\n   Title :  " + JSON.stringify(event_info.title) + "\n   Salary Range :  " + JSON.stringify(event_info.salary) + "\n \n  Skill :  " + JSON.stringify(event_info.skill)
			};

			//	console.log(JSON.stringify(config));

			let email = catalystApp.email();
			let mailPromise = email.sendMail(config);
			console.log('Mail is Sent now --------------------- ');
			mailPromise.then((mailObject) => {
				console.log('MAIL  Sent ' + JSON.stringify(mailObject));
				context.closeWithSuccess();
			}).catch(err => {
				console.log('error while sending the mail' + err);
				context.closeWithFailure();
			});
		}

	}
	dropOtherRows(catalystApp, context);
	//	context.closeWithSuccess();
}

function dropOtherRows(catalystApp, context) {
	console.log('----------- ABOUT TO DROP OTHER ROWS ----------------');
	let datastore = catalystApp.datastore();
	let table = datastore.table('NasaJobs');

	let q_zcql = catalystApp.zcql();
	let zcqlPromise = q_zcql.executeZCQLQuery("SELECT * FROM NasaJobs where location !='Greenbelt' ");
	zcqlPromise.then(queryResult => {

		//	console.log(' -------------   ' + JSON.stringify(queryResult));
		if (queryResult.length > 0) {
			let promiseArr = [];
			for (i = 0; i < queryResult.length; i++) {
				//	console.log('ROWID IS ... ' + queryResult[i].NasaJobs.ROWID);
				let rowPromise = table.deleteRow(queryResult[i].NasaJobs.ROWID);
				promiseArr.push(rowPromise);
			};

			Promise.all(promiseArr).then(function (values) {
				//		console.log(values);
				context.closeWithSuccess();
			}).catch(error => {
				console.log('error occurred while deleting row' + error);
				context.closeWithFailure();
			})
		}
	})
}
