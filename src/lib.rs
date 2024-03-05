use anyhow::{anyhow, Result};
use polars::prelude::*;
use sqlparser::parser::Parser;
use std::convert::TryInto;
use std::ops::{Deref, DerefMut};
use tracing::info;


mod dialect;
mod convert;
mod fetcher;
mod loader;

use convert::Sql;
use fetcher::retrieve_data;
use loader::detect_content;
pub use dialect::example_sql;
pub use dialect::CalDialect;

#[derive(Debug)]
pub struct DataSet(DataFrame);

/// 让 DataSet用起来和DataFrame一致
impl Deref for DataSet{
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl DataSet {
    /// 从 DataSet转换为 csv
    pub fn to_csv(&self) -> Result<String> {
        let mut buf = Vec::new();
        let writer = CsvWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }
}


/// 从 from中获取数据， 从where中过滤，最后选取需要返回的列
pub async fn query<T: AsRef<str>>(sql: T) -> Result<DataSet> {
    let ast = Parser::parse_sql(&CalDialect::default(), sql.as_ref())?;
    
    if ast.len() != 1 {
        return Err(anyhow!("Only support single sql at the moment"));
    }
    
    let sql = &ast[0];
    
    // 整个SQL AST转换成我们定义的Sql结构的细节都埋藏在try_into()中
    // 我们只需要关注数据结构的使用，怎么转换可以之后需要的时候才关注
    // 这就是关注点分离，是控制软件复杂度的法宝
    let Sql {
        source,
        condition,
        selection,
        offset,
        limit,
        order_by,
    } = sql.try_into()?;
    
    info!("retrieve data from source: {}", source);
    
    // 从source读入一个DataSet
    // detect_content， 怎么detect不重要，重要的是它能根据内容返回DataSet
    let ds = detect_content(retrieve_data(source).await?).load()?;
    
    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };
    
    filtered = order_by
        .into_iter()
        .fold(filtered, |acc, (col, desc)| acc.sort(&col, desc));
    
    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX));
    }
    Ok(DataSet(filtered.select(selection).collect()?))
}






















