use lazy_static::lazy_static;

use crate::{api_versions::ApiKey, common_struct::TagBuffer};

lazy_static! {
    pub static ref FETCH_API_INFO: ApiKey = ApiKey::new(1, 0, 16, TagBuffer::default());
}
