use std::time::Duration;

use crate::protocol::Coordinate;

impl Coordinate {
    pub fn estimate_rtt(&self, other: &Coordinate) -> Duration {
        // calculate euclidean distance
        let dist = self.vec.iter().zip(other.vec.iter()).map(|(l, r)| {
            (*l - *r).powi(2)
        }).sum::<f32>().sqrt();
        
        // add heights
        let unadjusted = dist + self.height + other.height;

        // adjust by adjustment factors
        let adjusted = unadjusted + self.adjustment + other.adjustment;
        
        // guard against negative adjustments
        let rtt = if adjusted.is_sign_positive() { adjusted } else { unadjusted };

        Duration::from_secs_f32(rtt)
    }
}