-- =============================================================================
-- VN30 ETL Pipeline — dim_stocks seed data
-- Danh mục 30 mã cổ phiếu rổ VN30 (HOSE), cập nhật Q1-2026
-- =============================================================================

INSERT INTO vn30.dim_stocks (stock_key, company_name, exchange, sector_name, industry_name) VALUES
    ('ACB',  'Ngân hàng TMCP Á Châu',                               'HOSE', 'Tài chính', 'Ngân hàng'),
    ('BID',  'Ngân hàng TMCP Đầu tư và Phát triển VN (BIDV)',       'HOSE', 'Tài chính', 'Ngân hàng'),
    ('CTG',  'Ngân hàng TMCP Công Thương VN (VietinBank)',           'HOSE', 'Tài chính', 'Ngân hàng'),
    ('HDB',  'Ngân hàng TMCP Phát triển TP. Hồ Chí Minh',           'HOSE', 'Tài chính', 'Ngân hàng'),
    ('MBB',  'Ngân hàng TMCP Quân đội',                             'HOSE', 'Tài chính', 'Ngân hàng'),
    ('SHB',  'Ngân hàng TMCP Sài Gòn - Hà Nội',                    'HOSE', 'Tài chính', 'Ngân hàng'),
    ('SSB',  'Ngân hàng TMCP Đông Nam Á (SeABank)',                  'HOSE', 'Tài chính', 'Ngân hàng'),
    ('STB',  'Ngân hàng TMCP Sài Gòn Thương Tín (Sacombank)',       'HOSE', 'Tài chính', 'Ngân hàng'),
    ('TCB',  'Ngân hàng TMCP Kỹ thương VN (Techcombank)',           'HOSE', 'Tài chính', 'Ngân hàng'),
    ('TPB',  'Ngân hàng TMCP Tiên Phong (TPBank)',                   'HOSE', 'Tài chính', 'Ngân hàng'),
    ('VCB',  'Ngân hàng TMCP Ngoại thương VN (Vietcombank)',        'HOSE', 'Tài chính', 'Ngân hàng'),
    ('VIB',  'Ngân hàng TMCP Quốc tế VN',                           'HOSE', 'Tài chính', 'Ngân hàng'),
    ('VPB',  'Ngân hàng TMCP Việt Nam Thịnh Vượng (VPBank)',        'HOSE', 'Tài chính', 'Ngân hàng'),
    ('SSI',  'Công ty CP Chứng khoán SSI',                          'HOSE', 'Tài chính', 'Chứng khoán'),
    ('BVH',  'Tập đoàn Bảo Việt',                                   'HOSE', 'Tài chính', 'Bảo hiểm'),
    ('BCM',  'Tổng Công ty Đầu tư và Phát triển Công nghiệp',       'HOSE', 'Bất động sản', 'Khu công nghiệp'),
    ('VHM',  'Công ty CP Vinhomes',                                  'HOSE', 'Bất động sản', 'Nhà ở'),
    ('VIC',  'Tập đoàn Vingroup',                                    'HOSE', 'Bất động sản', 'Đa ngành'),
    ('VRE',  'Công ty CP Vincom Retail',                             'HOSE', 'Bất động sản', 'Bán lẻ'),
    ('FPT',  'Công ty CP FPT',                                       'HOSE', 'Công nghệ', 'CNTT'),
    ('MWG',  'Công ty CP Đầu tư Thế Giới Di Động',                  'HOSE', 'Hàng tiêu dùng', 'Bán lẻ điện tử'),
    ('MSN',  'Công ty CP Tập đoàn Masan',                           'HOSE', 'Hàng tiêu dùng', 'Thực phẩm & Bán lẻ'),
    ('SAB',  'Tổng Công ty CP Bia - Rượu - NGK Sài Gòn (Sabeco)',  'HOSE', 'Hàng tiêu dùng', 'Đồ uống'),
    ('VJC',  'Công ty CP Hàng không VietJet',                        'HOSE', 'Hàng tiêu dùng', 'Hàng không'),
    ('VNM',  'Công ty CP Sữa VN (Vinamilk)',                        'HOSE', 'Hàng tiêu dùng', 'Thực phẩm'),
    ('GVR',  'Tập đoàn Công nghiệp Cao su VN',                     'HOSE', 'Vật liệu', 'Cao su'),
    ('HPG',  'Công ty CP Tập đoàn Hòa Phát',                        'HOSE', 'Vật liệu', 'Thép'),
    ('GAS',  'Tổng Công ty Khí VN (PV GAS)',                       'HOSE', 'Năng lượng', 'Khí đốt'),
    ('PLX',  'Tập đoàn Xăng dầu VN (Petrolimex)',                  'HOSE', 'Năng lượng', 'Xăng dầu'),
    ('POW',  'Tổng Công ty Điện lực Dầu khí VN',                   'HOSE', 'Năng lượng', 'Điện');
