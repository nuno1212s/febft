mod socket;

//use super::context::Context;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeId(u32);

//pub struct Node;

// Add more backends:
// ==================
// #[cfg(feature = "foo_bar_backend")]
// pub struct Node(...);
//
// ...

//impl Node {
//    pub fn id(&self) -> NodeId {
//        self.0.id()
//    }
//
//    pub async fn send(&self, ctx: &Context, 
//}
