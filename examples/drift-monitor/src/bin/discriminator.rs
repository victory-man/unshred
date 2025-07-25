// Calculate discriminators for instructions
use sha2::{Digest, Sha256};

fn calculate_discriminator(instruction_name: &str) -> [u8; 8] {
    let preimage = format!("global:{}", instruction_name);
    let hash = Sha256::digest(preimage.as_bytes());
    let mut discriminator = [0u8; 8];
    discriminator.copy_from_slice(&hash[0..8]);
    discriminator
}

fn main() {
    let instructions = [
        "liquidate_spot",
        "liquidate_perp",
        "liquidate_spot_with_swap_begin",
        "liquidate_spot_with_swap_end",
        "liquidate_borrow_for_perp_pnl",
        "liquidate_perp_pnl_for_deposit",
        "resolve_spot_bankruptcy",
        "resolve_perp_bankruptcy",
        "liquidate_perp_with_fill",
        "set_user_status_to_being_liquidated",
    ];

    for instruction in instructions {
        let discriminator = calculate_discriminator(instruction);
        println!("{}: {:?}", instruction, discriminator);
    }
}
