// You are to calculate monthly retention month over month for the data provided

// You can access the data here 

// Write a simple solution with any language of your choice to print out the monthly 
// retention (number of users retained from the previous month)

// postgres://
// uddmfcekkt42ui
// :p539645ff8350c7034e87644aa5dcb3643cc90f141c9c79deacf04cd90dce63bf
// @cc0gj7hsrh0ht8.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com
// :5432/
// d3ca7csmihkq2t
const {Client} = require('pg')
// postgres://uddmfcekkt42ui:
// p539645ff8350c7034e87644aa5dcb3643cc90f141c9c79deacf04cd90dce63bf@
// cc0gj7hsrh0ht8.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:
// 5432/
// d3ca7csmihkq2t
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

// Users 
// {
//     user_id: 1,
//     email: 'Elmer_Sanford@yahoo.com',
//     created_at: 2024-10-19T02:09:42.097Z,
//     updated_at: 2024-10-22T19:08:53.060Z
//   },

// {
//     transaction_id: 5,
//     user_id: 82,
//     created_at: 2024-03-31T19:00:00.000Z,
//     updated_at: 2024-03-31T19:00:00.000Z,
//     amount: '45.03'
//   }

// You are to calculate monthly retention month over month for the data provided
// Write a simple solution with any language of your choice to print out the monthly 
// retention (number of users retained from the previous month)
// 01 | user1 
async function dbConnect(){
    await conn.connect()
    const sql = `

        SELECT 
            month,
            COUNT(month_m_month) total_retained
        FROM (
            SELECT 
                EXTRACT(MONTH from created_at ) as month,
                user_id,
                count(transaction_id) as transaction_id,
                CASE 
                    WHEN 
                        LEAD(user_id) OVER (PARTITION BY USER_ID ORDER BY EXTRACT(MONTH from created_at )) IS NULL THEN 'NO'
                        ELSE 'YES'
                END as month_m_month

            FROM TRANSACTIONS 
            WHERE EXTRACT(YEAR from created_at )
            GROUP BY MONTH, USER_ID 
            HAVING COUNT(transaction_id) > 1 

        ) as TB
        WHERE month_m_month = 'YES'
        GROUP BY month 
    
    `
    const query = await conn.query(sql);
    const {rows} = query
    console.table(rows)


}

dbConnect()

