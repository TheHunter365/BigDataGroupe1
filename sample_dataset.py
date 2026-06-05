import os
import json
import random
import time

def sample_dataset():
    # Set seed for reproducibility
    random.seed(42)

    # Define sampling rate (0.0015 = 0.15% to keep total dataset size strictly under 50MB)
    SAMPLE_RATE = 0.0015

    # Base paths
    root_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(root_dir, "app", "dataset")
    output_dir = os.path.join(root_dir, "data")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    print(f"🚀 Starting Yelp Dataset Sampling ({SAMPLE_RATE*100}% Homogenous)...")
    print(f"Input Directory: {input_dir}")
    print(f"Output Directory: {output_dir}\n")

    # 1. Sample Businesses
    biz_input_path = os.path.join(input_dir, "yelp_academic_dataset_business.json")
    biz_output_path = os.path.join(output_dir, "yelp_academic_dataset_business.json")
    
    selected_biz_ids = set()
    biz_total = 0
    biz_sampled = 0

    t0 = time.time()
    print(f"⏳ Step 1: Sampling {SAMPLE_RATE*100}% of Businesses...")
    if not os.path.exists(biz_input_path):
        print(f"❌ Error: Business file not found at {biz_input_path}")
        return

    with open(biz_input_path, "r", encoding="utf-8") as fin, open(biz_output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            biz_total += 1
            biz = json.loads(line)
            # Sample using configured rate
            if random.random() < SAMPLE_RATE:
                fout.write(line)
                selected_biz_ids.add(biz["business_id"])
                biz_sampled += 1
    
    print(f"✅ Step 1 Complete: Sampled {biz_sampled:,} / {biz_total:,} businesses ({biz_sampled/biz_total*100:.2f}%)")
    print(f"   Elapsed time: {time.time() - t0:.2f}s\n")

    # 2. Sample Reviews for the selected businesses
    rev_input_path = os.path.join(input_dir, "yelp_academic_dataset_review.json")
    rev_output_path = os.path.join(output_dir, "yelp_academic_dataset_review.json")
    
    selected_user_ids = set()
    rev_total = 0
    rev_sampled = 0

    t0 = time.time()
    print("⏳ Step 2: Extracting Reviews for Sampled Businesses...")
    if not os.path.exists(rev_input_path):
        print(f"❌ Error: Review file not found at {rev_input_path}")
        return

    with open(rev_input_path, "r", encoding="utf-8") as fin, open(rev_output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            rev_total += 1
            if rev_total % 1000000 == 0:
                print(f"   Processed {rev_total:,} reviews...")
            
            review = json.loads(line)
            if review.get("business_id") in selected_biz_ids:
                fout.write(line)
                selected_user_ids.add(review["user_id"])
                rev_sampled += 1

    print(f"✅ Step 2 Complete: Kept {rev_sampled:,} / {rev_total:,} reviews ({rev_sampled/rev_total*100:.2f}%)")
    print(f"   Collected {len(selected_user_ids):,} unique user IDs")
    print(f"   Elapsed time: {time.time() - t0:.2f}s\n")

    # 3. Sample User Profiles for the collected users
    user_input_path = os.path.join(input_dir, "yelp_academic_dataset_user.json")
    user_output_path = os.path.join(output_dir, "yelp_academic_dataset_user.json")
    
    user_total = 0
    user_sampled = 0

    t0 = time.time()
    print("⏳ Step 3: Extracting User Profiles for Collected Users...")
    if not os.path.exists(user_input_path):
        print(f"❌ Error: User file not found at {user_input_path}")
        return

    with open(user_input_path, "r", encoding="utf-8") as fin, open(user_output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            user_total += 1
            if user_total % 500000 == 0:
                print(f"   Processed {user_total:,} users...")
                
            user = json.loads(line)
            if user.get("user_id") in selected_user_ids:
                fout.write(line)
                user_sampled += 1

    print(f"✅ Step 3 Complete: Kept {user_sampled:,} / {user_total:,} user profiles ({user_sampled/user_total*100:.2f}%)")
    print(f"   Elapsed time: {time.time() - t0:.2f}s\n")

    # 4. Optional / Homogenous Tips for the selected businesses
    tip_input_path = os.path.join(input_dir, "yelp_academic_dataset_tip.json")
    tip_output_path = os.path.join(output_dir, "yelp_academic_dataset_tip.json")
    
    if os.path.exists(tip_input_path):
        tip_total = 0
        tip_sampled = 0
        t0 = time.time()
        print("⏳ Step 4: Extracting Tips for Sampled Businesses...")
        with open(tip_input_path, "r", encoding="utf-8") as fin, open(tip_output_path, "w", encoding="utf-8") as fout:
            for line in fin:
                tip_total += 1
                tip = json.loads(line)
                if tip.get("business_id") in selected_biz_ids:
                    fout.write(line)
                    tip_sampled += 1
        print(f"✅ Step 4 Complete: Kept {tip_sampled:,} / {tip_total:,} tips ({tip_sampled/tip_total*100:.2f}%)")
        print(f"   Elapsed time: {time.time() - t0:.2f}s\n")

    # 5. Optional / Homogenous Checkins for the selected businesses
    checkin_input_path = os.path.join(input_dir, "yelp_academic_dataset_checkin.json")
    checkin_output_path = os.path.join(output_dir, "yelp_academic_dataset_checkin.json")
    
    if os.path.exists(checkin_input_path):
        checkin_total = 0
        checkin_sampled = 0
        t0 = time.time()
        print("⏳ Step 5: Extracting Checkins for Sampled Businesses...")
        with open(checkin_input_path, "r", encoding="utf-8") as fin, open(checkin_output_path, "w", encoding="utf-8") as fout:
            for line in fin:
                checkin_total += 1
                checkin = json.loads(line)
                if checkin.get("business_id") in selected_biz_ids:
                    fout.write(line)
                    checkin_sampled += 1
        print(f"✅ Step 5 Complete: Kept {checkin_sampled:,} / {checkin_total:,} checkins ({checkin_sampled/checkin_total*100:.2f}%)")
        print(f"   Elapsed time: {time.time() - t0:.2f}s\n")

    print("🎉 Dataset Sampling Complete! All homogenous subsets saved to the 'data/' directory.")

if __name__ == "__main__":
    sample_dataset()
