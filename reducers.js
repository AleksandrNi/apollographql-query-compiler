const {imageUploader} = require('../cloudinary/imageUploader')
const jwt = require('jsonwebtoken')
const config = require('config');
const format = require('pg-format');

const db = require ('../libs/pg');

exports.resolvers = {
    Query: {
        category: async (_, {id, title}, {ctx, user}, info ) => {
            let {fieldRow, tableLeftJoinRow} = queryParamsFunc({info, parentName: 'categories'})
            const sql = format(`
            SELECT %s 
            FROM %s
            WHERE categories.category_uid::text iLIKE '%s'
            AND categories.title::text iLIKE '%s' `, fieldRow, tableLeftJoinRow, id + '%', title );
            
            return await db.query(sql)  
            .then(res=>filteredResRows({res, parentName:'categories', parentNameIndexKey: 'categoryId', user})[0])
            .catch(err=>console.error(err))
        },

    },
    User: {
        posts: async (parent, args, _ , info) => {            
            return parent['posts']
        },
        categories: async (parent) => {
            return parent['categories']
        },
    },
    Post: {
        author: async (parent) => {
            return parent['users_table'][0]
        },
        categories: async (parent) => {     
            return parent['categories'][0]
        },
    },
    Category: {
        author: async (parent, args, _ , info) => {   
            return parent['users_table'][0]
        },
        posts: async (parent, args, _ , info) => {
            return parent['posts']
        },
    },

}



const sectionsmapForQueryFields = ({selections, parentName, deep}) => {
    // how deep in should work
    if(deep > 3) return ;
    // parentName - entity name (categories or posts ...)
    // sections - list of queryparams like title, author (complex), etc 

    return selections.map(selection => {
        
        if(selection.selectionSet) {
            //  correlate query entity with database table name 
            let tableName = selection.name.value;
            switch (tableName) {
                case 'users':
                    tableName = 'users_table'
                    break;
                case 'author':
                    tableName = 'users_table'
                    break;
            }

            return sectionsmapForQueryFields({
                selections: selection.selectionSet.selections,
                parentName: parentName + ':'+ '{' + tableName + '}',
                deep: deep + 1
            })

        } else {   
           
            return parentName + '.' + selection.name.value
                // entity queryParams
                // posts {
                //     title,
                //     text
                // }
                // parentName: posts
                // return posts_title
                // then
                // return posts_text

        }

    });
}

// queryFilds
// [ '{categories}.title',
// [ '{categories}:{posts}_title',
// [ '{categories}:{posts}_{author}_name' ] ] ]
const cleanQuery = (queryFilds) => {
    const queryList = [];  
    cleanQueryBody(queryFilds, queryList)
    return queryList;
}

const cleanQueryBody = (queryFilds, queryList) => {
    queryFilds.forEach(item=>Array.isArray(item)
        ? cleanQueryBody(item, queryList)
        : queryList.push(item)
    )
}

const EntitiesId = {
    req: {
        categories : 'category_uid',
        posts : 'post_uid',
        users_table : 'name'
    },
    res: {
        categories : 'categoryId',
        posts : 'postId',
        users_table : 'name'
    }
    
}

const EntitiesIdList = ['categoryId','postId', 'name']

// tables dependencies
const EntitiesRelationships = {
    posts: {
        users_table: 'user_uid',
        categories: 'category_uid',
    },
    categories: {
        users_table: 'user_uid',
        posts: 'category_uid'
    },
    users_table: {
        categories: 'user_uid',
        posts: 'user_uid'
    },
};

const queryParamsFunc = ({info, parentName}) => {    
    let queryFilds =  sectionsmapForQueryFields({
        selections: info.fieldNodes[0].selectionSet.selections,
        parentName: `{${parentName}}`,
        deep: 0
    })

//     queryFilds

// [ '{categories}.title',
//   '{categories}:{posts}.title',
//   '{categories}:{posts}.published',
//   '{categories}:{posts}:{users:table}.name' ]

    queryFilds = cleanQuery(queryFilds)
    
    // 'users_table.user_uid AS "userId"'

    // [ 'categories.title',
    // 'categories:posts.title',
    // 'categories:posts.published',
    // 'categories:posts:users:table.name' ]
    
    // remove curly braces
    let fields = queryFilds.map(field=>field.replace(/\{|\}/gi, '')).filter(field=>field.indexOf('__typename') === -1)
    
    // correlate query fields with databace table columns
    
    // [ 'categories.title',
    // 'categories:posts.title',
    // 'categories:posts.published',
    // 'categories:posts:users_table.first_name AS categories:posts:users_table.name' ]

    fields = fields.map((field, index)=>{
        let [corePart, query] = field.split('.');
        
        if(corePart.indexOf(':') === -1) {
            const coreQuery = coreQueryFunc ({corePart, query});
            
            return index === 0
            ? coreQuery + ( corePart !== 'users_table' ? `, ${corePart}.${EntitiesId.req[corePart]}` : `, users_table.first_name AS name`)
            : coreQuery;
            
        } else {
            
            // replace : for __ just for readable and sql
            corePart = corePart.replace(/\:/gi, '__')
            // let renamedCorePart = renamedCorePartFunc(corePart)
            
            switch (query) {
                case 'userId':              return corePart + `.user_uid AS "${corePart}___userId"`
                case 'isPostAuthor':        return corePart + `.user_uid AS "isPostAuthor"`
                case 'postId':              return corePart + `.post_uid AS "${corePart}___postId"`
                case 'categoryId':          return corePart + `.category_uid AS "${corePart}___categoryId"`
                case 'subTitle':            return corePart + `.subtitle AS "${corePart}___subTitle"`
                case 'structuredText':      return corePart + `.structured_text AS "${corePart}___structuredText"`
                case 'structuredTextPreview':return corePart + `.structured_text_preview AS "${corePart}___structuredTextPreview"`
                case 'postOnIndex':         return corePart + `.post_on_index AS "${corePart}___postOnIndex"`
                case 'text':        
                    switch (corePart) {
                        case 'categories':  return corePart + `.category_text AS ${corePart}___text`
                    }

                case 'name':                return corePart + `.first_name AS ${corePart}___name`
                case 'created':             return `to_char(${corePart}.created, 'MM.DD.YY') AS ${corePart}___created`
                case 'registered':          return `to_char(${corePart}.registered, 'MM.DD.YY') AS ${corePart}___registered`
                case 'modified':            return `to_char(${corePart}.modified, 'MM.DD.YY') AS ${corePart}___modified`
                default:                    return corePart + `.${query} AS ${corePart}___${query}`;
            }
        }
    })
    
    // let tableName;


    // remove doubles
    const buffer = []
    let tableLeftJoin = queryFilds
    .map(item=>item
        .replace(/\.\w+/, '')
        .replace(/\{|\}/gi, ''))
    .filter(item=>buffer.includes(item)  
        ? (false)  
        : (buffer.push(item) && true) 
        )

    
    if (tableLeftJoin[0] !== parentName) {
        tableLeftJoin = tableLeftJoin.filter(i=>i!==tableLeftJoin)
        tableLeftJoin = [parentName, ...tableLeftJoin]
    }
        

    tableLeftJoin = tableLeftJoin.map(item=>{
        
        item = item.indexOf(':') === -1
            ? [item]
            : item.split(':');

        const deep = item.length
        let alias;
        let alias2;
        
        let entityKey;
        switch (deep) {
            case 1: return item[0].replace(/\{|\}/gi, '');

            case 2:
                alias = item.join('__');
                
                //                      categories      posts
                entityKey = EntitiesRelationships[item[0]][item[1]]
                return `
                LEFT JOIN ${item[1]} AS ${alias} ON ${item[0]}.${entityKey} = ${alias}.${entityKey}`;
            case 3:

                alias = item.join('__');
                            // on categories__posts
                alias2 = item.slice(0,2).join('__');

                //                      posts           users
                entityKey = EntitiesRelationships[item[1]][item[2]]
                return `
                LEFT JOIN ${item[2]} AS ${alias} ON ${alias2}.${entityKey} = ${alias}.${entityKey}`;
        
            default: return item;
        }
    })
    
     
    const fieldRow = fields.join(', ')
    const tableLeftJoinRow = tableLeftJoin.join(' ');
    return {
        fieldRow,
        tableLeftJoinRow,
        queryFilds
        // varString
    }
}


const filteredResRows = ({res, parentName, parentNameIndexKey, user}) => {
    // '_'      - reserved for table names
    // '__'     - tables deep splitter
    // '___'    - query splitter

    const rows = res.rows;
    
    
    
    const formattedData = [];
    
    
    for (let i = 0; i < rows.length; i++) {
        
        
        
        let row = rows[i]
        const rowKeys = Object.keys(row)

        // check if entity already exists in formattedData list
        let containerIndex = isContainerIndex ({formattedData, row, parentName});
     

        // remove private information
        row = cleanUpPrivateInfo({row, user})
        
        const resBody = prepareResTemplate({rowKeys});
        
        for (let j = 0; j < rowKeys.length; j++) {
            const key = rowKeys[j]
            

            // root query fields
            if (key.indexOf('__') === -1) {
                resBody[key] = row[key]

            // deep query fields
            } else {
                const query = rowKeys
                    .filter(rowKey=>rowKey.indexOf(key) !== -1)[0]
                    .split('___')[1];

                const keyParts = key
                .replace(/\___\w+/, '')
                .split('__')
                .slice(1);
                                
                if(row[key] === null) continue;

                keyParts.length === 1
                    ? resBody[keyParts[0]][0][query] = row[key]
                    : resBody[keyParts[0]][0][keyParts[1]][0][query] = row[key];

            // row
            // { title: 'Next',
            //   category_uid: '7bd10735-9f30-46bd-9dd2-e687cc1a6774',
            //   categories__users___name: 'Sasha',
            //   categories__posts___title: 'New test article ',
            //   categories__posts___published: true,
            //   categories__posts__users___name: 'Sasha' }

            // resBody
            // { users: [ { name: 'Sasha' } ],
            //   posts:
            //    [ { users: [{name: 'Sasha'}], title: 'New test article ', published: true } ],
            //   title: 'Next',
            //   category_uid: '7bd10735-9f30-46bd-9dd2-e687cc1a6774' }
                

            }
        }

        const resBodyKeys = Object.keys(resBody)
        for(let r=0;r<resBodyKeys.length;r++){
            if(Array.isArray(resBody[resBodyKeys[r]])) {
                
   
                
                let emptyObj = false;
                const listItemKeys = Object.keys(resBody[resBodyKeys[r]][0] );

                if(!listItemKeys.length) emptyObj = true;
                for (let e=0; e<listItemKeys.length; e++){
                    const listItemKeysKey = listItemKeys[e]
                    
                    const value = resBody[resBodyKeys[r]][0][listItemKeysKey]
              
                    if(Array.isArray(value) && !Object.keys(value[0]).length) emptyObj = true;
                    break;
                }         
                if(emptyObj) resBody[resBodyKeys[r]] = [];
            }
        }

        if(containerIndex !== -1) {
            const sameRes = formattedData[containerIndex];
            deepJoining({sameRes,resBody});

        } else {
            formattedData.push(resBody)
        }
    }

    return formattedData
    
}

function prepareResTemplate ({rowKeys}) {
    let resTemplate = {};
    for (let i = 0; i < rowKeys.length; i++) {
        let key = rowKeys[i];
        if (key.indexOf('__') === -1) continue;
        const keyParts = key.replace(/\___\w+/, '').split('__').slice(1);

        // FOR 2 or 3 LEVEL {
        //     categories: {
        //         posts: {
        //             authors: 'Sasha'
        //         }
        //     }
        // }
        
        keyParts.forEach((part,index)=>{    
            if(index === 0) {
                resTemplate[part] = []
                resTemplate[part][0] = {};
            } else {
                resTemplate[keyParts[0]][0] = {};
                resTemplate[keyParts[0]][0][part] = []
                resTemplate[keyParts[0]][0][part][0] = {};
            }
        })
    }

    // { users_table: [], posts: [ { users_table: [{}] } ] }    
    return resTemplate;
}

function isContainerIndex ({formattedData, row, parentName}) {
    let containerIndex = -1;

    const currentEntityId = EntitiesId.res[parentName]
    
    for (let k = 0; k < formattedData.length; k++) {
        
        if(formattedData[k] && formattedData[k][currentEntityId] === row[currentEntityId]) {
            containerIndex = k;
        }
    }
    return containerIndex
}

function cleanUpPrivateInfo ({row, user}) {
    const rowKeys = Object.keys(row)
        
    for (let j = 0; j < rowKeys.length; j++) {
        const rowKey = rowKeys[j]

        //  check is user have permission to private information like user id or email
        if( rowKey.match(/email/) ) {
            row[rowKey] = user && row[rowKey] === user.email 
            ? row[rowKey]
            : null
        }
        if( rowKey.match(/isPostAuthor/) ) {
            row[rowKey] = user ? user.user_uid === row['isPostAuthor'] : false;
        }
        if( rowKey.match(/userId|user_uid/) ) {
            row[rowKey] = null; 
        }
    }
    return row
}

function deepJoining ({sameRes, resBody}) {
    
    const sameReskeys = Object.keys(sameRes)

    for(let s = 0; s < sameReskeys.length; s++) {
        if(typeof sameRes[sameReskeys[s]] !== 'object' || sameRes[sameReskeys[s]] === null) continue;
        
        // sameReskeys
        // [ 'categories', 'name', 'email' ]
        // sameRes[sameReskeys[s]]
        // [ { categoryId: '9070e175-014d-48e3-adf0-f8ad9e993c2a' }]

        
        if (Array.isArray(sameRes[sameReskeys[s]])) {     
            const currentEntityId = EntitiesId.res[sameReskeys[s]];
            let isItemInSameRes = false;
            const entityList = sameRes[sameReskeys[s]];   

            for(let se=0; se<entityList.length; se++) {
                if(resBody[sameReskeys[s]][0][currentEntityId] === null) continue;

                 if(resBody[sameReskeys[s]][0][currentEntityId] && entityList[se][currentEntityId] === resBody[sameReskeys[s]][0][currentEntityId] ) {
                    isItemInSameRes = true;
                }
            }

            if(!isItemInSameRes) {
                sameRes[sameReskeys[s]] = sameRes[sameReskeys[s]].concat(resBody[sameReskeys[s]])
            }

            
            continue;

        }
        const sameResChildKeys = Object.keys(sameRes[sameReskeys[s]])

        for (let c = 0; c < sameResChildKeys.length; c++) {
            if( typeof sameRes[sameReskeys[s]][sameResChildKeys[c]] !== 'object' || sameRes[sameReskeys[s]][sameResChildKeys[c]] === null) continue;
            if(Array.isArray(sameRes[sameReskeys[s]][sameResChildKeys[c]])) {                        
                sameRes[sameReskeys[s]][sameResChildKeys[c]] = sameRes[sameReskeys[s]][sameResChildKeys[c]].concat(resBody[sameReskeys[s]][sameResChildKeys[c]])
                continue;
            } else {continue;}
        }
    }
}

function coreQueryFunc ({corePart, query}) {
    switch (query) {
        case 'userId':              return corePart + `.user_uid AS "userId"`
        case 'isPostAuthor':        return corePart + `.user_uid AS "isPostAuthor"`
        case 'postId':              return corePart + `.post_uid AS "postId"` 
        case 'categoryId':          return corePart + `.category_uid AS "categoryId"`
        case 'subTitle':            return corePart + `.subtitle AS "subTitle"`
        case 'structuredText':      return corePart + `.structured_text AS "structuredText"`
        case 'structuredTextPreview':return corePart + `.structured_text_preview AS "structuredTextPreview"`
        case 'postOnIndex':         return corePart + `.post_on_index AS "postOnIndex"`
        case 'text':        
            switch (corePart) {
                case 'categories':  return corePart + `.category_text AS text`
            }

        case 'name':                return corePart + `.first_name AS name`
        case 'created':             return `to_char(${corePart}.created, 'MM.DD.YY') AS created`
        case 'registered':          return `to_char(${corePart}.registered, 'MM.DD.YY') AS registered`
        case 'modified':            return `to_char(${corePart}.modified, 'MM.DD.YY') AS modified`
        default:                    return corePart + `.${query} AS ${query}`;
    }
}
