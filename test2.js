const {Client} = require('pg');
const moment = require('moment');
// postgres://uddmfcekkt42ui:p539645ff8350c7034e87644aa5dcb3643cc90f141c9c79deacf04cd90dce63bf@cc0gj7hsrh0ht8.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d3ca7csmihkq2t

const conn = new Client({
    host: 'cc0gj7hsrh0ht8.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
    user: 'uddmfcekkt42ui',
    password: 'p539645ff8350c7034e87644aa5dcb3643cc90f141c9c79deacf04cd90dce63bf',
    port: 5432,
    database: 'd3ca7csmihkq2t',
    ssl: {
        rejectUnauthorized: false
    }
})



async function dbConnect(){
    await conn.connect()

    const sql = `
        SELECT 
            created_at,
            user_id,
            transaction_id
        FROM 
            TRANSACTIONS    
        ;
    `
    const query = await conn.query(sql);
    const {rows} = query;    
    // Total transaction record : 10,000
    
    traverseRow(rows)
    conn.end();
}

function traverseRow(data = []){
    const selectedYear = 2024
    const dataMapper = {};
    dataMapper[selectedYear] = [];

    // steps taken here ;
    // Filter by year(2024)
    // group by month 
    // Filter by year 2024 , group by month 
    const filteredYear = selectedYear ? data.filter( row => moment(row['created_at']).year() == selectedYear) : data

    const groupByYearMonth = {}
    for (let index = 0; index < data.length; index++) {
        const row = data[index];
        const rowYear = moment(row['created_at']).year();
        const rowMonth = moment(row['created_at']).month() + 1;

        // Filter for 2024
        if ( rowYear == selectedYear ) {
            // Group by 
            const groupByYearMonthKey = `${rowMonth}:${row['user_id']}`;

            if(groupByYearMonth[groupByYearMonthKey]){
                groupByYearMonth[groupByYearMonthKey]['totalTransaction'] += 1 // group by & count 
            } else {
                groupByYearMonth[groupByYearMonthKey] = {
                    ...row,
                    totalTransaction: 1, // group by & count 
                }

            }
        }
    }


    // Calculate Month In Month 
    for (const key in groupByYearMonth) {
        if (Object.hasOwnProperty.call(groupByYearMonth, key)) {
            const groupByYearMonthRow = groupByYearMonth[key];

            // increment month by 1, to check if there's a transaction for the user in the next month
            // Key = MONTH : USER_ID
            const nextMonthKey = `${parseInt(key.split(':')[0], 10) + 1}:${key.split(':')[1]}`;

            if(groupByYearMonth[nextMonthKey]) {
                groupByYearMonth[key]['monthInMonth'] = 'YES'
            } else {
                groupByYearMonth[key]['monthInMonth'] = 'NO'
            }
            
            
        }
    }

    const sortedgroupByYearMonthKeys = Object.keys(groupByYearMonth).sort();
    const sortedObject = {};
    sortedgroupByYearMonthKeys.forEach(key => {
        sortedObject[key] = groupByYearMonth[key];
    });

    // Final aggregate
    const FinalAggregate = {}
    for (const key in sortedObject) {
        if (Object.hasOwnProperty.call(sortedObject, key)) {
            const row = sortedObject[key];
            if (row['monthInMonth'] == 'YES'){

                if (FinalAggregate[key.split(':')[0]] ){
                    FinalAggregate[ key.split(':')[0] ]['totalRetained'] += 1
                } else {
                    // total_retained
                    FinalAggregate[key.split(':')[0]] = {
                        totalRetained: 1
                    };
                }

            }            
        }
    }

    console.log('Final dataset', FinalAggregate )
    console.table(FinalAggregate)
    // ┌─────────┬───────────────┐
    // │ (index) │ totalRetained │
    // ├─────────┼───────────────┤
    // │    1    │      100      │
    // │    2    │      100      │
    // │    3    │      100      │
    // │    4    │      100      │
    // └─────────┴───────────────┘





    

    



}

dbConnect().catch(console.error);