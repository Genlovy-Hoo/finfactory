# -*- coding: utf-8 -*-

import pandas as pd


def cal_opt_vol_to_buy(Pcost, Vhold, Pnow, target_loss_rate=30/100, fee=12):
    '''
    | （买方）
    | 计算加仓量，Pcost为当前成本价（不包含手续费）、Vhold为当前持仓量，Pnow为当前价格
    | target_loss_rate为加仓后亏损目标
    '''
    
    target_Pcost = Pnow / (1-target_loss_rate) # 成本目标
    # 加仓量
    vol = ((Pcost+fee) * Vhold - Vhold*target_Pcost) / (target_Pcost-Pnow-fee)
    vol = round(vol)
    
    Total_cost = vol * (Pnow+fee) + Vhold * (Pcost+fee) # 总成本
    Total_hold = (Vhold + vol) * Pnow # 加仓后总价值
    Pcost_new = Total_cost / (Vhold + vol) # 新成本
    loss_rate_new = Pnow / Pcost_new - 1 # 新亏损比例
    
    return vol, loss_rate_new, Pcost_new, (Total_cost, Total_hold)


def cal_opt_vol_to_sel(Pcost, Vhold, Pnow, target_loss_rate=30/100, fee=12):
    '''
    | （卖方）（未仔细检查，可能有误！）
    | 计算加仓量，Pcost为当前成本价（不包含手续费）、Vhold为当前持仓量，Pnow为当前价格
    | target_loss_rate为加仓后亏损目标
    '''
    
    target_Pcost = Pnow / (1+target_loss_rate) # 成本目标
    # 加仓量
    vol = ((Pcost-fee) * Vhold - Vhold*target_Pcost) / (target_Pcost-Pnow+fee)
    vol = round(vol)
    
    Total_cost = vol * (Pnow-fee) + Vhold * (Pcost-fee) # 总成本
    Total_hold = (Vhold + vol) * Pnow # 加仓后总价值
    Pcost_new = Total_cost / (Vhold + vol) # 新成本
    loss_rate_new = Pnow / Pcost_new - 1 # 新亏损比例
    
    return vol, loss_rate_new, Pcost_new, (Total_cost, Total_hold)


def cal_opt_vol_to_buy_new(Pcost_Vhold_r_Pnow, Pnow, target_loss_rate=30/100,
                           fee=12):
    '''
    | 计算开（加）量
    | Pcost_Vhold_r_Pnow格式：
    |     [(Pcost, Vhold, r, Pnow), (...), ...]，
    |     Pcost为成本价（不含手续费）、Vhold为持仓量、r为剩余价值折算比例、Pnow为现价
    '''
    
    hold_cost = sum([(x[0]+fee)*x[1] for x in Pcost_Vhold_r_Pnow]) # 持仓总成本
    hold_left = sum([x[1]*x[2]*x[3] for x in Pcost_Vhold_r_Pnow]) # 折算剩余价值
    # 开（加）仓量
    vol = (hold_cost*(1-target_loss_rate) - hold_left) / \
                                      (Pnow - (1-target_loss_rate)*(fee+Pnow))
    vol = round(vol)
    
    # 总成本
    Total_cost = vol * (fee+Pnow) + hold_cost
    # 折算后持有价值
    Total_hold = vol * Pnow + hold_left
    loss_rate_new = Total_hold / Total_cost - 1
    
    return vol, loss_rate_new, (Total_cost, Total_hold)


if __name__ == '__main__':
    Vhold = 1
    Nmax = 10
    loss_ctrl = 2.0/100
    Ppre = 1
    Pcost = 1
    records_sel = [[Pcost, Vhold, Vhold, Ppre]]
    for k in range(1, Nmax):
        Pnow = Ppre * (1 + loss_ctrl)
        vol, loss_rate_new, Pcost_new, (Total_cost, Total_hold) = \
                            cal_opt_vol_to_sel(Pcost, Vhold, Pnow,
                                               target_loss_rate=0.5/100, fee=0)
        Ppre = Pnow
        Pcost = Pcost_new
        Vhold += vol
        records_sel.append([Pcost, vol, Vhold, Pnow])
    records_sel = pd.DataFrame(records_sel, columns=['当前成本', '交易量',
                                                     '持仓量', '现价'])
    
    
    Vhold = 1
    Nmax = 10
    loss_ctrl = 2.0/100
    Ppre = 1
    Pcost = 1
    records_buy = [[Pcost, Vhold, Vhold, Ppre]]
    for k in range(1, Nmax):
        Pnow = Ppre * (1 - loss_ctrl)
        vol, loss_rate_new, Pcost_new, (Total_cost, Total_hold) = \
                            cal_opt_vol_to_buy(Pcost, Vhold, Pnow,
                                               target_loss_rate=0.5/100, fee=0)
        Ppre = Pnow
        Pcost = Pcost_new
        Vhold += vol
        records_buy.append([Pcost, vol, Vhold, Pnow])
    records_buy = pd.DataFrame(records_buy, columns=['当前成本', '交易量',
                                                     '持仓量', '现价'])
    
