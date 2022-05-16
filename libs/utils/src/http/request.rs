use std::str::FromStr;

use super::error::ApiError;
use hyper::{body::HttpBody, Body, Request};
use routerify::ext::RequestExt;

pub fn get_request_param<'a>(
    request: &'a Request<Body>,
    param_name: &str,
) -> Result<&'a str, ApiError> {
    match request.param(param_name) {
        Some(arg) => Ok(arg),
        None => {
            return Err(ApiError::BadRequest(format!(
                "no {} specified in path param",
                param_name
            )))
        }
    }
}

pub fn parse_request_param<T: FromStr>(
    request: &Request<Body>,
    param_name: &str,
) -> Result<T, ApiError> {
    match get_request_param(request, param_name)?.parse() {
        Ok(v) => Ok(v),
        Err(_) => Err(ApiError::BadRequest(format!(
            "failed to parse {}",
            param_name
        ))),
    }
}

pub async fn ensure_no_body(request: &mut Request<Body>) -> Result<(), ApiError> {
    match request.body_mut().data().await {
        Some(_) => Err(ApiError::BadRequest("Unexpected request body".into())),
        None => Ok(()),
    }
}
