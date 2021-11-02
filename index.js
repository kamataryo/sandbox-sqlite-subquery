const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(':memory:');

const items = [
	{ session_id: 'a', data: 'x', status_code: 200 },
	{ session_id: 'a', data: 'y', status_code: 200 },
	{ session_id: 'a', data: 'z', status_code: 200 },
	{ session_id: 'b', data: 'x', status_code: 403 },
	{ session_id: 'b', data: 'y', status_code: 200 },
	{ session_id: 'b', data: 'z', status_code: 200 },
	{ session_id: 'c', data: 'x', status_code: 403 },
	{ session_id: 'c', data: 'y', status_code: 200 },
	{ session_id: 'c', data: 'z', status_code: 403 },
	{ session_id: 'd', data: 'x', status_code: 403 },
	{ session_id: 'd', data: 'y', status_code: 403 },
	{ session_id: 'd', data: 'z', status_code: 403 },
	{ session_id: 'e', data: 'x', status_code: 403 },
	{ session_id: 'e', data: 'y', status_code: 403 },
	{ session_id: 'e', data: 'z', status_code: 403 },
]

db.serialize(() => {
	db.run(`CREATE TABLE test_table (
		session_id text, 
		data text, 
		status_code text
	)`);

	const stmt = db.prepare("INSERT INTO test_table VALUES (?, ?, ?)");
	items.forEach(item => {
		stmt.run(`${item.session_id}`, `${item.data}`, `${item.status_code}`)
	});
	stmt.finalize();

	db.each(`SELECT
		count(DISTINCT (CASE WHEN unfulfilled_request_count = 0 THEN session_id END)) AS count,
		count(DISTINCT (CASE WHEN unfulfilled_request_count > 0 THEN session_id END)) AS unfulfilled_count
	FROM (
		SELECT
			session_id,
			count(status_code = 403 OR NULL) as unfulfilled_request_count
		FROM test_table
		WHERE status_code in (200, 403)
		GROUP BY session_id
	)
	`, (err, row) => {
		if(err) {
			throw err;
		} else {
			console.log(row);
		}
	});

});

db.close();