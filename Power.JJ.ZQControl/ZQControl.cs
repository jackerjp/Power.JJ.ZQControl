using Newtonsoft.Json;
using Power.Configure;
using Power.Controls.Action;
using Power.Controls.PMS;
using Power.Global;
using Power.IBaseCore;
using Power.Message;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Linq;
using System.Data.SqlClient;

namespace Power.JJ.ZQControl
{    
    public class ZQControl : BaseControl
    {
          string sWBS = @"insert into pln_projwbsZQ (
                    wbs_id,wbs_guid,proj_id,proj_guid,obs_id,obs_guid,seq_num,
                    est_wt,complete_pct,proj_node_flag,sum_data_flag,status_code,
                    wbs_short_name,wbs_name,phase_id,parent_wbs_id,parent_wbs_guid,
                    ev_user_pct,ev_etc_user_value,orig_cost,user_cost1,indep_remain_total_cost,
                    ann_dscnt_rate_pct,dscnt_period_type,indep_remain_work_qty,anticip_start_date,
                    anticip_end_date,ev_compute_type,ev_etc_compute_type,guid,tmpl_guid,p3ec_wbs_id,
                    p3ec_parentwbs_id,p3ec_flag,treelevel,haschild,discolor,target_start_date,target_end_date,
                    expect_start_date,expect_end_date,act_start_date,act_end_date,SysOrNot,allowModifyTaskOrNot,
                    complete_pct_type,target_drtn_hr_cnt,remain_drtn_hr_cnt,update_date,start_date,end_date,
                    LongCode,plan_id,plan_guid,est_wt_pct,require_start_date,require_end_date,main_wbs_guid,bcws_cost,
                    bcwp_cost,acwp_cost,budget_cost,reghumanid,reghumanname,rsrc_guid,rsrc_name,responsibleid,responsiblename,
                    update_user,create_date,create_user,delete_session_id,delete_date,target_complete_pct,period_target_complete_pct,
                    restart_date,reend_date,plan_pct,wbs_guid_before,dept_name,dept_guid,remark,plan_hour,act_hour,MasterId)
                    select wbs_id,wbs_guid,proj_id,proj_guid,obs_id,obs_guid,seq_num,
                    est_wt,complete_pct,proj_node_flag,sum_data_flag,status_code,
                    wbs_short_name,wbs_name,phase_id,parent_wbs_id,parent_wbs_guid,
                    ev_user_pct,ev_etc_user_value,orig_cost,user_cost1,indep_remain_total_cost,
                    ann_dscnt_rate_pct,dscnt_period_type,indep_remain_work_qty,anticip_start_date,
                    anticip_end_date,ev_compute_type,ev_etc_compute_type,guid,tmpl_guid,p3ec_wbs_id,
                    p3ec_parentwbs_id,p3ec_flag,treelevel,haschild,discolor,target_start_date,target_end_date,
                    expect_start_date,expect_end_date,act_start_date,act_end_date,SysOrNot,allowModifyTaskOrNot,
                    complete_pct_type,target_drtn_hr_cnt,remain_drtn_hr_cnt,update_date,start_date,end_date,
                    LongCode,plan_id,plan_guid,est_wt_pct,require_start_date,require_end_date,main_wbs_guid,bcws_cost,
                    bcwp_cost,acwp_cost,budget_cost,reghumanid,reghumanname,rsrc_guid,rsrc_name,responsibleid,responsiblename,
                    update_user,create_date,create_user,delete_session_id,delete_date,target_complete_pct,period_target_complete_pct,
                    restart_date,reend_date,plan_pct,wbs_guid_before,dept_name,dept_guid,remark,plan_hour,act_hour,@MasterId from pln_projwbs
                    where plan_guid=@plan_guid ";

         string sZY = @"insert into pln_taskZQ(task_id,task_guid,proj_id,proj_guid,wbs_id,wbs_guid,clndr_id,clndr_guid,
                        plan_id,plan_guid,seq_num,parent_task_id,parent_guid,est_wt,complete_pct,rev_fdbk_flag,lock_plan_flag,
                        auto_compute_act_flag,complete_pct_type,task_type,duration_type,review_type,status_code,task_code,
                        task_name,rsrc_name,rsrc_id,rsrc_guid,total_float_hr_cnt,free_float_hr_cnt,target_drtn_hr_cnt,act_drtn_hr_cnt,
                        remain_drtn_hr_cnt,act_work_qty,remain_work_qty,target_work_qty,target_equip_qty,act_equip_qty,remain_equip_qty,
                        cstr_type,cstr_date,late_start_date,late_end_date,early_start_date,early_end_date,restart_date,reend_date,review_end_date,
                        rem_late_start_date,rem_late_end_date,priority_type,guid,tmpl_guid,cstr_date2,cstr_type2,act_this_per_work_qty,act_this_per_equip_qty,
                        driving_path_flag,float_path,float_path_order,suspend_date,resume_date,external_early_start_date,external_late_end_date,update_date
                        ,update_user,create_date,create_user_sid,create_user,delete_session_id,delete_date,act_start_date,act_end_date,expect_start_date,expect_end_date,
                        target_start_date,target_end_date,SysOrNot,rec_type,temp_id,p3ec_flag,p3ec_task_id,data_date,start_date,end_date,plan_task_id_befor,plan_task_guid_before,
                        memo,module_type,plan_actvcode_guid,plan_actvcode_id,parent_task_plan_guid,est_wt_pct,curve_guid,feedback_pct_type,baseline_start_date,baseline_end_date,
                        baseline2_start_date,baseline2_end_date,baseline3_start_date,baseline3_end_date,progress_rsrc_unit_name,progress_rsrc_unit_price,target_progress_rsrc_cost,
                        act_progress_rsrc_cost,remain_progress_rsrc_cost,target_progress_rsrc_curv,act_progress_rsrc_curv,remain_progress_rsrc_curv,target_progress_rsrc_guid,target_progress_rsrc_code,
                        target_progress_rsrc_qty,act_progress_rsrc_qty,remain_progress_rsrc_qty,videoUrl,bcws_cost,bcwp_cost,acwp_cost,istopbreakdown,budget_cost,attach_flag,UPDATE_SESSION_ID,
                        target_complete_pct,period_target_complete_pct,important_node_flag,plan_pct,feedback_data_date,dept_name,dept_guid,remark,plan_hour,act_hour,TempTask_code,MasterId)
                        select task_id,task_guid,proj_id,proj_guid,wbs_id,wbs_guid,clndr_id,clndr_guid,
                        plan_id,plan_guid,seq_num,parent_task_id,parent_guid,est_wt,complete_pct,rev_fdbk_flag,lock_plan_flag,
                        auto_compute_act_flag,complete_pct_type,task_type,duration_type,review_type,status_code,task_code,
                        task_name,rsrc_name,rsrc_id,rsrc_guid,total_float_hr_cnt,free_float_hr_cnt,target_drtn_hr_cnt,act_drtn_hr_cnt,
                        remain_drtn_hr_cnt,act_work_qty,remain_work_qty,target_work_qty,target_equip_qty,act_equip_qty,remain_equip_qty,
                        cstr_type,cstr_date,late_start_date,late_end_date,early_start_date,early_end_date,restart_date,reend_date,review_end_date,
                        rem_late_start_date,rem_late_end_date,priority_type,guid,tmpl_guid,cstr_date2,cstr_type2,act_this_per_work_qty,act_this_per_equip_qty,
                        driving_path_flag,float_path,float_path_order,suspend_date,resume_date,external_early_start_date,external_late_end_date,update_date
                        ,update_user,create_date,create_user_sid,create_user,delete_session_id,delete_date,act_start_date,act_end_date,expect_start_date,expect_end_date,
                        target_start_date,target_end_date,SysOrNot,rec_type,temp_id,p3ec_flag,p3ec_task_id,data_date,start_date,end_date,plan_task_id_befor,plan_task_guid_before,
                        memo,module_type,plan_actvcode_guid,plan_actvcode_id,parent_task_plan_guid,est_wt_pct,curve_guid,feedback_pct_type,baseline_start_date,baseline_end_date,
                        baseline2_start_date,baseline2_end_date,baseline3_start_date,baseline3_end_date,progress_rsrc_unit_name,progress_rsrc_unit_price,target_progress_rsrc_cost,
                        act_progress_rsrc_cost,remain_progress_rsrc_cost,target_progress_rsrc_curv,act_progress_rsrc_curv,remain_progress_rsrc_curv,target_progress_rsrc_guid,target_progress_rsrc_code,
                        target_progress_rsrc_qty,act_progress_rsrc_qty,remain_progress_rsrc_qty,videoUrl,bcws_cost,bcwp_cost,acwp_cost,istopbreakdown,budget_cost,attach_flag,UPDATE_SESSION_ID,
                        target_complete_pct,period_target_complete_pct,important_node_flag,plan_pct,feedback_data_date,dept_name,dept_guid,remark,plan_hour,act_hour,TempTask_code,@MasterId from pln_task
                        where plan_guid=@plan_guid";

         string sZQTask = @"select Id,MasterId,type,task_guid,task_code,task_name,target_start_date,
                            target_end_date,target_drtn_hr_cnt,est_wt_pct,act_start_date,act_end_date,
                            expect_end_date,remain_drtn_hr_cnt,last_complete_pct,complete_pct,ParentId,
                            feedback_pct_type,isApproved,UpdDate,Sequ,progress_rsrc_code,progress_rsrc_unit_name,
                            target_progress_rsrc_qty,progress_rsrc_unit_price,target_progress_rsrc_cost,plan_progress_rsrc_qty,
                            act_progress_rsrc_qty,plan_progress_rsrc_cost,act_progress_rsrc_cost,total_progress_rsrc_qty,
                            total_progress_rsrc_cost,period_complete_pct,task_type,clndr_guid,expect_start_date,wbs_long_path 
                            from PS_PLN_FeedBackRecord_Task where masterid=@MasterId and type='task'";

         string sUpdate = @"update pln_taskZQ set target_start_date=@target_start_date,
                            target_end_date=@target_end_date,target_drtn_hr_cnt=@target_drtn_hr_cnt,est_wt_pct=@est_wt_pct,act_start_date=@act_start_date,act_end_date=@act_end_date,
                            expect_end_date=@expect_end_date,remain_drtn_hr_cnt=@remain_drtn_hr_cnt,complete_pct=@complete_pct,
                            feedback_pct_type=@feedback_pct_type,progress_rsrc_unit_name=@progress_rsrc_unit_name,
                            target_progress_rsrc_qty=@target_progress_rsrc_qty,progress_rsrc_unit_price=@progress_rsrc_unit_price,target_progress_rsrc_cost=@target_progress_rsrc_cost,
                            act_progress_rsrc_qty=@act_progress_rsrc_qty,act_progress_rsrc_cost=@act_progress_rsrc_cost,expect_start_date=@expect_start_date where task_guid=@task_guid and masterid=@MasterId ";

         string sFeedBack = @"select * from PS_PLN_FeedBackRecord where plan_guid=@plan_guid
                             order by period_begindate";
          static SqlDataBase dbSQL = new SqlDataBase();
        [Action(Authorize = false)]
        public  string CopyWBSAndZY(string MasterID,string plan_guid)
        {
            dbSQL.beginTran();
            try
            {
                Dictionary<string, string> BCW = new Dictionary<string, string>();
                int XH = 0;
                StringBuilder sFeedSQL = new StringBuilder();
                sFeedSQL.Clear();
                sFeedSQL.AppendLine(sFeedBack);
                dbSQL.addParam("plan_guid", plan_guid);
                DataSet dsFeed = dbSQL.getDataSet(sFeedSQL.ToString());
                for (int i = 0; i < dsFeed.Tables[0].Rows.Count; i++)
                {
                    BCW.Add(dsFeed.Tables[0].Rows[i]["Id"].ToString(), dsFeed.Tables[0].Rows[i]["period_guid"].ToString());
                }
                for (int i = 0; i < dsFeed.Tables[0].Rows.Count; i++)
                {
                    if (MasterID == dsFeed.Tables[0].Rows[i]["Id"].ToString())
                    {
                        XH = i;
                        break;
                    }
                }
                if (XH == 0)
                {
                    //插入wbs
                    InsertWBS(MasterID, plan_guid);
                    //插入作业
                    InsertZY(MasterID, plan_guid);

                    StringBuilder sSQL = new StringBuilder();
                    StringBuilder sUpSQL = new StringBuilder();
                    sSQL.Clear();
                    sSQL.AppendLine(sZQTask);
                    dbSQL.addParam("MasterId", MasterID);
                    DataSet dsSQL = dbSQL.getDataSet(sSQL.ToString());
                    for (int i = 0; i < dsSQL.Tables[0].Rows.Count; i++)
                    {
                        sUpSQL.Clear();
                        sUpSQL.AppendLine(sUpdate);
                        dbSQL.addParam("target_start_date", dsSQL.Tables[0].Rows[i]["target_start_date"].ToString());
                        dbSQL.addParam("target_end_date", dsSQL.Tables[0].Rows[i]["target_end_date"].ToString());
                        dbSQL.addParam("target_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["target_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("est_wt_pct", dsSQL.Tables[0].Rows[i]["est_wt_pct"].ToString());
                        dbSQL.addParam("act_start_date", dsSQL.Tables[0].Rows[i]["act_start_date"].ToString());
                        dbSQL.addParam("act_end_date", dsSQL.Tables[0].Rows[i]["act_end_date"].ToString());
                        dbSQL.addParam("expect_end_date", dsSQL.Tables[0].Rows[i]["expect_end_date"].ToString());
                        dbSQL.addParam("remain_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["remain_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("complete_pct", dsSQL.Tables[0].Rows[i]["complete_pct"].ToString());
                        dbSQL.addParam("feedback_pct_type", dsSQL.Tables[0].Rows[i]["feedback_pct_type"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_name", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_name"].ToString());
                        dbSQL.addParam("target_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_price", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_price"].ToString());
                        dbSQL.addParam("target_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("act_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("act_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("expect_start_date", dsSQL.Tables[0].Rows[i]["expect_start_date"].ToString());
                        dbSQL.addParam("task_guid", dsSQL.Tables[0].Rows[i]["task_guid"].ToString());
                        dbSQL.addParam("MasterId", MasterID);
                        dbSQL.doSQL(sUpSQL.ToString());
                    }

                    //将计划分摊中的数据更新到作业周期表中
                    StringBuilder sBCWS = new StringBuilder();
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task' and plan_guid=@plan_guid");
                    dbSQL.addParam("period_guid", BCW[MasterID].ToString());//本期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    DataSet dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_taskZQ set plan_complete_pct=@plan_complete_pct,act_complete_pct=@act_complete_pct, ");
                        sUpBCWS.AppendLine("period_complete_pct=@period_complete_pct,period_Plan_complete_pct=@period_Plan_complete_pct,BY_Plan_complete_pct=@BY_Plan_complete_pct, ");
                        sUpBCWS.AppendLine("period_begindate=@period_begindate,period_enddate=@period_enddate,BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBCWS.AppendLine("  where masterid=@masterid and task_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        dbSQL.addParam("plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//计划完成百分比
                        dbSQL.addParam("act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//实际完成百分比
                        dbSQL.addParam("period_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期实际完成百分比
                        dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本月计划
                        dbSQL.addParam("period_begindate", dsFeed.Tables[0].Rows[0]["period_begindate"].ToString());//周期开始时间period_begindate
                        dbSQL.addParam("period_enddate", dsFeed.Tables[0].Rows[0]["period_enddate"].ToString());//周期结束时间

                        dbSQL.addParam("BN_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本年计划%
                        dbSQL.addParam("BN_act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//本年实际%

                        dbSQL.doSQL(sUpBCWS.ToString());
                    }

                    //将计划分摊中的数据更新到作业周期表中(WBS)
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='wbs'and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", BCW[MasterID].ToString());//本期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_projwbsZQ set plan_complete_pct=@plan_complete_pct,act_complete_pct=@act_complete_pct, ");
                        sUpBCWS.AppendLine("period_complete_pct=@period_complete_pct,period_Plan_complete_pct=@period_Plan_complete_pct,BY_Plan_complete_pct=@BY_Plan_complete_pct,BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBCWS.AppendLine("where masterid=@masterid and wbs_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        dbSQL.addParam("plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//计划完成百分比
                        dbSQL.addParam("act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//实际完成百分比
                        dbSQL.addParam("period_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期实际完成百分比                      
                        dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本月计划  

                        dbSQL.addParam("BN_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本月计划  
                        dbSQL.addParam("BN_act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//本月计划  
                        
                        dbSQL.doSQL(sUpBCWS.ToString());
                    }
                }
                else//如果不是第一期数据，则需要将前一期的数据先更新到表单中，然后再将本期的数据更新到表单中，得到最新的表单
                {
                    //插入wbs
                    InsertWBS(MasterID, plan_guid);
                    //插入作业
                    InsertZY(MasterID, plan_guid);

                    StringBuilder sUpSQL = new StringBuilder();
                    StringBuilder sSQL = new StringBuilder();

                    //将上一期的更新
                    sSQL.Clear();
                    sSQL.AppendLine(sZQTask);
                    dbSQL.addParam("MasterId", dsFeed.Tables[0].Rows[XH - 1]["Id"].ToString());//上一期的masterid
                    DataSet dsSQL = dbSQL.getDataSet(sSQL.ToString());
                    for (int i = 0; i < dsSQL.Tables[0].Rows.Count; i++)
                    {
                        sUpSQL.Clear();
                        sUpSQL.AppendLine(sUpdate);
                        dbSQL.addParam("target_start_date", dsSQL.Tables[0].Rows[i]["target_start_date"].ToString());
                        dbSQL.addParam("target_end_date", dsSQL.Tables[0].Rows[i]["target_end_date"].ToString());
                        dbSQL.addParam("target_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["target_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("est_wt_pct", dsSQL.Tables[0].Rows[i]["est_wt_pct"].ToString());
                        dbSQL.addParam("act_start_date", dsSQL.Tables[0].Rows[i]["act_start_date"].ToString());
                        dbSQL.addParam("act_end_date", dsSQL.Tables[0].Rows[i]["act_end_date"].ToString());
                        dbSQL.addParam("expect_end_date", dsSQL.Tables[0].Rows[i]["expect_end_date"].ToString());
                        dbSQL.addParam("remain_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["remain_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("complete_pct", dsSQL.Tables[0].Rows[i]["complete_pct"].ToString());
                        dbSQL.addParam("feedback_pct_type", dsSQL.Tables[0].Rows[i]["feedback_pct_type"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_name", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_name"].ToString());
                        dbSQL.addParam("target_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_price", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_price"].ToString());
                        dbSQL.addParam("target_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("act_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("act_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("expect_start_date", dsSQL.Tables[0].Rows[i]["expect_start_date"].ToString());
                        dbSQL.addParam("task_guid", dsSQL.Tables[0].Rows[i]["task_guid"].ToString());
                        dbSQL.addParam("MasterId", MasterID);
                        dbSQL.doSQL(sUpSQL.ToString());
                    }

                    //更新本期的
                    sSQL.Clear();
                    sSQL.AppendLine(sZQTask);
                    dbSQL.addParam("MasterId", MasterID);//本期的masterid
                    dsSQL = dbSQL.getDataSet(sSQL.ToString());
                    for (int i = 0; i < dsSQL.Tables[0].Rows.Count; i++)
                    {
                        sUpSQL.Clear();
                        sUpSQL.AppendLine(sUpdate);
                        dbSQL.addParam("target_start_date", dsSQL.Tables[0].Rows[i]["target_start_date"].ToString());
                        dbSQL.addParam("target_end_date", dsSQL.Tables[0].Rows[i]["target_end_date"].ToString());
                        dbSQL.addParam("target_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["target_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("est_wt_pct", dsSQL.Tables[0].Rows[i]["est_wt_pct"].ToString());
                        dbSQL.addParam("act_start_date", dsSQL.Tables[0].Rows[i]["act_start_date"].ToString());
                        dbSQL.addParam("act_end_date", dsSQL.Tables[0].Rows[i]["act_end_date"].ToString());
                        dbSQL.addParam("expect_end_date", dsSQL.Tables[0].Rows[i]["expect_end_date"].ToString());
                        dbSQL.addParam("remain_drtn_hr_cnt", dsSQL.Tables[0].Rows[i]["remain_drtn_hr_cnt"].ToString());
                        dbSQL.addParam("complete_pct", dsSQL.Tables[0].Rows[i]["complete_pct"].ToString());
                        dbSQL.addParam("feedback_pct_type", dsSQL.Tables[0].Rows[i]["feedback_pct_type"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_name", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_name"].ToString());
                        dbSQL.addParam("target_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("progress_rsrc_unit_price", dsSQL.Tables[0].Rows[i]["progress_rsrc_unit_price"].ToString());
                        dbSQL.addParam("target_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["target_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("act_progress_rsrc_qty", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_qty"].ToString());
                        dbSQL.addParam("act_progress_rsrc_cost", dsSQL.Tables[0].Rows[i]["act_progress_rsrc_cost"].ToString());
                        dbSQL.addParam("expect_start_date", dsSQL.Tables[0].Rows[i]["expect_start_date"].ToString());
                        dbSQL.addParam("task_guid", dsSQL.Tables[0].Rows[i]["task_guid"].ToString());
                        dbSQL.addParam("MasterId", MasterID);
                        dbSQL.doSQL(sUpSQL.ToString());
                    }


                    int Currmonth = DateTime.Parse(dsFeed.Tables[0].Rows[XH]["period_enddate"].ToString()).Month;//此周期的当前月
                    int CurrYear = DateTime.Parse(dsFeed.Tables[0].Rows[XH]["period_enddate"].ToString()).Year;//此周期的当前年
                    #region  本月计划%
                    StringBuilder sFeed = new StringBuilder();
                    sFeed.Clear();
                    sFeed.AppendLine("select * from PS_PLN_FeedBackRecord where plan_guid=@plan_guid ");
                    sFeed.AppendLine("and year(period_enddate)=@year and month(period_enddate)=@month ");
                    sFeed.AppendLine(" order by period_begindate");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("year", CurrYear);
                    dbSQL.addParam("month", Currmonth);
                    DataSet dsRecord = dbSQL.getDataSet(sFeed.ToString());
                    int FeedXH = 0;
                    for (int i = 0; i < dsRecord.Tables[0].Rows.Count; i++)//查询本次周期落在本月周期中的第几个
                    {
                        if (MasterID == dsRecord.Tables[0].Rows[i]["ID"].ToString())
                        {
                            FeedXH = i;
                            break;
                        }
                    }

                    //所有周期中最大和最小值
                    StringBuilder sMax = new StringBuilder();
                    sMax.Clear();
                    sMax.AppendLine("select task_guid,isnull(max(act_complete_pct),0) as act_complete_pct,isnull(max(plan_complete_pct),0) as plan_complete_pct,isnull(min(act_complete_pct),0) as minact_complete_pct ,isnull(min(plan_complete_pct),0) as minplan_complete_pct   from PS_PLN_TaskBCWS where period_guid in( ");
                    sMax.AppendLine("  select period_guid from PS_PLN_FeedBackRecord where plan_guid=@plan_guid  ");
                    sMax.AppendLine(" and exists(select 1 from pln_taskZQ A where A.masterid=PS_PLN_FeedBackRecord.id)) ");
                    sMax.AppendLine("  group by task_guid");
                    dbSQL.addParam("plan_guid", plan_guid);
                    DataSet dsSYZQMax = dbSQL.getDataSet(sMax.ToString());


                    //今年周期中最大最小的数据
                    sMax.Clear();
                    sMax.AppendLine("select task_guid,isnull(max(act_complete_pct),0) as act_complete_pct,isnull(max(plan_complete_pct),0) as plan_complete_pct,isnull(min(act_complete_pct),0) as minact_complete_pct ,isnull(min(plan_complete_pct),0) as minplan_complete_pct   from PS_PLN_TaskBCWS where period_guid in( ");
                    sMax.AppendLine("  select period_guid from PS_PLN_FeedBackRecord where plan_guid=@plan_guid and year(period_enddate)=@year ");
                    sMax.AppendLine(" and exists(select 1 from pln_taskZQ A where A.masterid=PS_PLN_FeedBackRecord.id)) ");
                    sMax.AppendLine("  group by task_guid");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("year", CurrYear);
                    DataSet dsBNMax = dbSQL.getDataSet(sMax.ToString());

                    //本年本月最大最小的数据
                    sMax.Clear();
                    sMax.AppendLine(" select task_guid,isnull(max(act_complete_pct),0) as act_complete_pct,isnull(max(plan_complete_pct),0) as plan_complete_pct,isnull(min(act_complete_pct),0) as minact_complete_pct ,isnull(min(plan_complete_pct),0) as minplan_complete_pct   from PS_PLN_TaskBCWS where period_guid in(  ");
                    sMax.AppendLine("  select period_guid from PS_PLN_FeedBackRecord where plan_guid=@plan_guid  ");
                    sMax.AppendLine(" and year(period_enddate)=@year and month(period_enddate)=@month  and exists(select 1 from pln_taskZQ A where A.masterid=PS_PLN_FeedBackRecord.id)) ");
                    sMax.AppendLine(" group by task_guid ");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("year", CurrYear);
                    dbSQL.addParam("month", Currmonth);
                    DataSet dsBNBYMax = dbSQL.getDataSet(sMax.ToString());

                    #endregion
                    ArrayList ZYList = new ArrayList();
                    ArrayList WBSList = new ArrayList();
                    //查出所有 的作业
                    StringBuilder sZY = new StringBuilder();
                    sZY.Clear();
                    sZY.AppendLine(" select  task_guid from pln_task where plan_guid=@plan_guid  ");
                    dbSQL.addParam("plan_guid", plan_guid);
                    DataSet dsZY = dbSQL.getDataSet(sZY.ToString());
                    for (int i = 0; i < dsZY.Tables[0].Rows.Count; i++)
                    {
                        ZYList.Add(dsZY.Tables[0].Rows[i]["task_guid"].ToString());
                    }

                    sZY.Clear();
                    sZY.AppendLine(" select wbs_guid from pln_projwbs where plan_guid=@plan_guid    ");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsZY = dbSQL.getDataSet(sZY.ToString());
                    for (int i = 0; i < dsZY.Tables[0].Rows.Count; i++)
                    {
                        WBSList.Add(dsZY.Tables[0].Rows[i]["wbs_guid"].ToString());
                    }

                    //将计划分摊中的数据更新到作业周期表中(作业)
                    StringBuilder sBCWS = new StringBuilder();
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task' and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", BCW[dsFeed.Tables[0].Rows[XH - 1]["Id"].ToString()].ToString());//上一期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    DataSet dsBCWS = dbSQL.getDataSet(sBCWS.ToString());

                        sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task'and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", BCW[MasterID].ToString());//本期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    DataSet dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        ZYList.Remove(dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        StringBuilder sFirst = new StringBuilder();
                        sFirst.Clear();
                        sFirst.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task' and plan_guid=@Firplan_guid ");
                        dbSQL.addParam("period_guid", BCW[dsRecord.Tables[0].Rows[0]["ID"].ToString()].ToString());//本周期第一个周期的masterid
                        dbSQL.addParam("Firplan_guid", plan_guid);
                        DataSet dsFirst = dbSQL.getDataSet(sFirst.ToString());

                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;
                        //将所有周期中最大的查询出来
                        for (int k = 0; k < dsSYZQMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsSYZQMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                            }                            
                        }                        

                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_taskZQ set plan_complete_pct=@plan_complete_pct,act_complete_pct=@act_complete_pct,BY_act_complete_pct=@BY_act_complete_pct,period_begindate=@period_begindate,period_enddate=@period_enddate,");
                        sUpBCWS.AppendLine("period_complete_pct=@period_complete_pct,period_Plan_complete_pct=@period_Plan_complete_pct,BY_Plan_complete_pct=@BY_Plan_complete_pct ");
                        sUpBCWS.AppendLine(" where masterid=@masterid and task_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        if (dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString().Equals("0"))
                            dbSQL.addParam("plan_complete_pct", maxplan_complete_pct);//计划完成百分比
                        else
                            dbSQL.addParam("plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//计划完成百分比
                        if (dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString().Equals("0"))
                            dbSQL.addParam("act_complete_pct", maxact_complete_pct);//实际完成百分比
                        else
                            dbSQL.addParam("act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//实际完成百分比
                        dbSQL.addParam("period_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期实际完成百分比
                        dbSQL.addParam("period_begindate", dsFeed.Tables[0].Rows[XH]["period_begindate"].ToString());//本期实际完成百分比
                        dbSQL.addParam("period_enddate", dsFeed.Tables[0].Rows[XH]["period_enddate"].ToString());//本期实际完成百分比
                        if (dsBCWS.Tables[0].Rows.Count > 0)//如果上期存在数据，则本期计划完成百分比等于累计计划完成百分比-上期累计计划完成百分比
                        {
                            string BYJHPct = "";
                            for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)//循环上期，比对作业的task_guid
                            {
                                
                                if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                {
                                    double plan_complete_pct = 0;
                                    plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                    dbSQL.addParam("period_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比
                                    BYJHPct = plan_complete_pct.ToString();
                                    break;
                                }
                                if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                {
                                    dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                                    BYJHPct = dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString();
                                }                                
                            }

                            #region 本月计划%本月完成%
                            if (FeedXH == 0 || FeedXH == 1)//说明这个周期落在第一个或者第二个，就要用本周期的数据减去上个周期得到本月计划%
                            {
                                for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)
                                {
                                    if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", BYJHPct);//本期计划完成百分比()实际就等于本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期实际完成百分比
                                        break;
                                    }
                                    if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["Plan_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期实际完成百分比
                                    }
                                }
                            }
                            else if (FeedXH > 1)//就要用本期数据减去第一期数据得到本月计划%
                            {
                                for (int j = 0; j < dsBNBYMax.Tables[0].Rows.Count; j++)//循环本年本月最大最小数据
                                {
                                    if (dsBNBYMax.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                    {
                                        double plan_complete_pct = 0;
                                        plan_complete_pct = double.Parse(dsBNBYMax.Tables[0].Rows[j]["act_complete_pct"].ToString()) - double.Parse(dsBNBYMax.Tables[0].Rows[j]["minact_complete_pct"].ToString());
                                        dbSQL.addParam("BY_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比

                                        double act_complete_pct = 0;
                                        act_complete_pct = double.Parse(dsBNBYMax.Tables[0].Rows[j]["plan_complete_pct"].ToString()) - double.Parse(dsBNBYMax.Tables[0].Rows[j]["minplan_complete_pct"].ToString());
                                        dbSQL.addParam("BY_act_complete_pct", act_complete_pct.ToString());//本期计划完成百分比
                                        break;
                                    }
                                    if (j == dsBNBYMax.Tables[0].Rows.Count - 1)
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", BYJHPct);//本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                                    }
                                }
                                //for (int j = 0; j < dsFirst.Tables[0].Rows.Count; j++)
                                //{
                                //    if (dsFirst.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))//第一个周期的task_guid等于本期的task_guid，则将本期的计划完成百分比减去第一期的计划完成百分比
                                //    {
                                //        double plan_complete_pct = 0;
                                //        plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsFirst.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                //        dbSQL.addParam("BY_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比


                                //        double act_complete_pct = 0;
                                //        act_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString()) - double.Parse(dsFirst.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                //        dbSQL.addParam("BY_act_complete_pct", act_complete_pct.ToString());//本期计划完成百分比
                                //        break;
                                //    }
                                //    if (j == dsFirst.Tables[0].Rows.Count - 1)
                                //    {
                                //        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                                //        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                                //    }
                                //}
                            }
                            #endregion                            
                        }
                        else//如果上期不存在数据，则本期计划完成就等于累计计划完成百分比
                        {
                            dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                            dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                            dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                        }
                        dbSQL.doSQL(sUpBCWS.ToString());
                    }

                    #region 本期作业以外的作业，
                    for (int i = 0; i < ZYList.Count; i++)//本期作业以外的作业，
                    {
                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;

                        //将所有周期中最大的查询出来
                        for (int k = 0; k < dsSYZQMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsSYZQMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(ZYList[i].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                            }
                        }

                        double BNmaxact_complete_pct = 0;
                        double BNmaxplan_complete_pct = 0;
                        double BNminact_complete_pct = 0;
                        double BNminplan_complete_pct = 0;
                        //将本年的最大最小值查询出来

                        for (int k = 0; k < dsBNMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsBNMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(ZYList[i].ToString()))
                            {
                                BNmaxact_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                BNmaxplan_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                                BNminact_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["minact_complete_pct"].ToString());
                                BNminplan_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["minplan_complete_pct"].ToString());
                            }
                        }

                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_taskZQ set plan_complete_pct=@plan_complete_pct,act_complete_pct=@act_complete_pct,BY_act_complete_pct=0,period_begindate=@period_begindate,period_enddate=@period_enddate,");
                        sUpBCWS.AppendLine(" BY_Plan_complete_pct=0,BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBCWS.AppendLine(" where masterid=@masterid and task_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", ZYList[i].ToString());
                        dbSQL.addParam("plan_complete_pct", maxplan_complete_pct);//计划完成百分比
                        dbSQL.addParam("act_complete_pct", maxact_complete_pct);//实际完成百分比
                        //dbSQL.addParam("period_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期实际完成百分比
                        dbSQL.addParam("period_begindate", dsFeed.Tables[0].Rows[XH]["period_begindate"].ToString());//本期实际完成百分比
                        dbSQL.addParam("period_enddate", dsFeed.Tables[0].Rows[XH]["period_enddate"].ToString());//本期实际完成百分比

                        dbSQL.addParam("BN_Plan_complete_pct", BNmaxplan_complete_pct- BNminplan_complete_pct);//本年计划完成%
                        dbSQL.addParam("BN_act_complete_pct", BNmaxact_complete_pct- BNminact_complete_pct);//本年实际完成%
                        dbSQL.doSQL(sUpBCWS.ToString());
                    }
                    #endregion

                    #region 至上年底累计完成%
                    int LastYear =DateTime.Parse(dsFeed.Tables[0].Rows[XH]["period_enddate"].ToString()).Year-1;//此周期的上一年
                    StringBuilder sYear = new StringBuilder();
                    sYear.Clear();
                    sYear.AppendLine("select * from PS_PLN_FeedBackRecord where plan_guid=@plan_guid ");
                    sYear.AppendLine("and year(period_enddate)=@year and period_enddate=(select  max(period_enddate) as period_enddate from PS_PLN_FeedBackRecord where year(period_enddate)=@period_enddate)");
                    sYear.AppendLine(" order by period_begindate");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("year", LastYear);
                    dbSQL.addParam("period_enddate", LastYear);
                    DataSet dsYear = dbSQL.getDataSet(sYear.ToString());
                    if (dsYear.Tables[0].Rows.Count > 0)
                    {
                        //更新作业的至上年底累计完成%
                        sYear.Clear();
                        sYear.AppendLine(" select * from PS_PLN_TaskBCWS where period_guid=@period_guid and type='task'");
                        dbSQL.addParam("period_guid", dsYear.Tables[0].Rows[0]["period_guid"].ToString());
                        DataSet TaskBCWS = dbSQL.getDataSet(sYear.ToString());
                        for (int i = 0; i < TaskBCWS.Tables[0].Rows.Count; i++)
                        {
                            sYear.Clear();
                            sYear.AppendLine("update pln_taskZQ set lastyear_complete_pct=@lastyear_complete_pct where masterid=@masterid and task_guid=@task_guid and plan_guid=@plan_guid");
                            dbSQL.addParam("masterid", MasterID);
                            dbSQL.addParam("plan_guid", plan_guid);
                            dbSQL.addParam("task_guid", TaskBCWS.Tables[0].Rows[i]["task_guid"].ToString());
                            dbSQL.addParam("lastyear_complete_pct", TaskBCWS.Tables[0].Rows[i]["act_complete_pct"].ToString());
                            dbSQL.doSQL(sYear.ToString());
                        }

                        //更新WBS的至上年底累计完成%
                        sYear.Clear();
                        sYear.AppendLine(" select max(act_complete_pct) as act_complete_pct,task_guid from PS_PLN_TaskBCWS where period_guid=@period_guid and type='wbs'  group by task_guid ");
                        dbSQL.addParam("period_guid", dsYear.Tables[0].Rows[0]["period_guid"].ToString());
                        TaskBCWS = dbSQL.getDataSet(sYear.ToString());
                        for (int i = 0; i < TaskBCWS.Tables[0].Rows.Count; i++)
                        {
                            sYear.Clear();
                            sYear.AppendLine("update pln_projwbsZQ set lastyear_complete_pct=@lastyear_complete_pct where masterid=@masterid and wbs_guid=@task_guid and plan_guid=@plan_guid");
                            dbSQL.addParam("masterid", MasterID);
                            dbSQL.addParam("plan_guid", plan_guid);
                            dbSQL.addParam("task_guid", TaskBCWS.Tables[0].Rows[i]["task_guid"].ToString());
                            dbSQL.addParam("lastyear_complete_pct", TaskBCWS.Tables[0].Rows[i]["act_complete_pct"].ToString());
                            dbSQL.doSQL(sYear.ToString());
                        }
                    }
                    else//没有上年度数据，则至上年底累计完成%为0
                    {
                        
                    }
                    #endregion

                    #region 本年计划%和本年实际完成%
                    Dictionary<string , string> ZQData = new Dictionary<string, string>();//记录存在数据的周期的id和period_guid
                    StringBuilder sBN = new StringBuilder();
                    sBN.Clear();
                    sBN.AppendLine(" select * from PS_PLN_FeedBackRecord where plan_guid=@plan_guid ");
                    sBN.AppendLine(" and year(period_enddate)=@Year and exists(select 1 from pln_taskZQ A where A.masterid=PS_PLN_FeedBackRecord.id) ");
                    sBN.AppendLine(" order by period_begindate ");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("Year", CurrYear);
                    DataSet dsBNdata = dbSQL.getDataSet(sBN.ToString());
                    for (int i = 0; i < dsBNdata.Tables[0].Rows.Count; i++)//查询本年度有多少期数据
                    {
                        ZQData.Add(dsBNdata.Tables[0].Rows[i]["Id"].ToString(), dsBNdata.Tables[0].Rows[i]["period_guid"].ToString());
                    }

                    #region  作业级
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task' and plan_guid=@plan_guid  ");
                    dbSQL.addParam("period_guid", ZQData[dsBNdata.Tables[0].Rows[0]["Id"].ToString()].ToString());//第一期的数据
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBCWS = dbSQL.getDataSet(sBCWS.ToString());

                    //将所有周期中固定月份中最大的查询出来
                    sMax = new StringBuilder();
                    sMax.Clear();
                    sMax.AppendLine(" select task_guid,isnull(max(act_complete_pct),0) as act_complete_pct,isnull(max(plan_complete_pct),0) as plan_complete_pct,isnull(min(act_complete_pct),0) as minact_complete_pct ,isnull(min(plan_complete_pct),0) as minplan_complete_pct   from PS_PLN_TaskBCWS where period_guid in(  ");
                    sMax.AppendLine("  select period_guid from PS_PLN_FeedBackRecord where plan_guid=@plan_guid  ");
                    sMax.AppendLine(" and year(period_enddate)=@year and month(period_enddate)<=@month  and exists(select 1 from pln_taskZQ A where A.masterid=PS_PLN_FeedBackRecord.id)) ");
                    sMax.AppendLine(" group by task_guid ");
                    dbSQL.addParam("plan_guid", plan_guid);
                    dbSQL.addParam("year", CurrYear);
                    dbSQL.addParam("month", Currmonth);
                    DataSet dsMax = dbSQL.getDataSet(sMax.ToString());

                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task'and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", ZQData[dsBNdata.Tables[0].Rows[dsBNdata.Tables[0].Rows.Count-1]["Id"].ToString()].ToString());//最后一期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;
                        double minact_complete_pct = 0;
                        double minplan_complete_pct = 0;


                        //将所有周期中固定月份中最大的查询出来
                        for (int k = 0; k < dsMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                                minact_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["minact_complete_pct"].ToString());
                                minplan_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["minplan_complete_pct"].ToString());
                            }
                        }

                        StringBuilder sUpBNBCWS = new StringBuilder();
                        sUpBNBCWS.Clear();
                        sUpBNBCWS.AppendLine("update pln_taskZQ set BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBNBCWS.AppendLine("where masterid=@masterid and task_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        if (dsBCWS.Tables[0].Rows.Count > 0)//如果第一期存在数据
                        {
                            for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)//循环第一期，比对作业的task_guid
                            {
                                if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                {
                                    double BN_Plan_complete_pct = 0;
                                    double BN_act_complete_pct = 0;
                                    BN_act_complete_pct= double.Parse(dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                    BN_Plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());   //最后一期的计划完成%减去第一期的计划完成%
                                    if (BN_act_complete_pct <= 0)
                                        BN_act_complete_pct = maxact_complete_pct - double.Parse(dsBCWS.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                    if (BN_Plan_complete_pct <= 0)
                                        BN_Plan_complete_pct = maxplan_complete_pct - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                    dbSQL.addParam("BN_Plan_complete_pct", BN_Plan_complete_pct.ToString());
                                    dbSQL.addParam("BN_act_complete_pct", BN_act_complete_pct.ToString());
                                    break;
                                }
                                if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                {
                                    dbSQL.addParam("BN_Plan_complete_pct", maxplan_complete_pct- minplan_complete_pct);
                                    dbSQL.addParam("BN_act_complete_pct", maxact_complete_pct- minact_complete_pct);
                                }
                            }
                        }
                        dbSQL.doSQL(sUpBNBCWS.ToString());
                    }
                    #endregion

                    #region  wbs级
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='wbs' and plan_guid=@plan_guid  ");
                    dbSQL.addParam("period_guid", ZQData[dsBNdata.Tables[0].Rows[0]["Id"].ToString()].ToString());//第一期的数据
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBCWS = dbSQL.getDataSet(sBCWS.ToString());

                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='wbs'and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", ZQData[dsBNdata.Tables[0].Rows[dsBNdata.Tables[0].Rows.Count - 1]["Id"].ToString()].ToString());//最后一期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;
                        double minact_complete_pct = 0;
                        double minplan_complete_pct = 0;

                        //将所有周期中最大的查询出来
                        //将所有周期中固定月份中最大的查询出来
                        for (int k = 0; k < dsMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                                minact_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["minact_complete_pct"].ToString());
                                minplan_complete_pct = double.Parse(dsMax.Tables[0].Rows[k]["minplan_complete_pct"].ToString());
                            }
                        }

                        StringBuilder sUpBNBCWS = new StringBuilder();
                        sUpBNBCWS.Clear();
                        sUpBNBCWS.AppendLine("update pln_projwbsZQ set BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBNBCWS.AppendLine("where masterid=@masterid and wbs_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        if (dsBCWS.Tables[0].Rows.Count > 0)//如果第一期存在数据
                        {
                            for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)//循环第一期，比对作业的task_guid
                            {
                                if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                {
                                    double BN_Plan_complete_pct = 0;
                                    double BN_act_complete_pct = 0;
                                    BN_act_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                    BN_Plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());   //最后一期的计划完成%减去第一期的计划完成%
                                    if (BN_act_complete_pct <= 0)
                                        BN_act_complete_pct = maxact_complete_pct - double.Parse(dsBCWS.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                    if(BN_Plan_complete_pct<=0)
                                        BN_Plan_complete_pct= maxplan_complete_pct - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                    dbSQL.addParam("BN_Plan_complete_pct", BN_Plan_complete_pct.ToString());
                                    dbSQL.addParam("BN_act_complete_pct", BN_act_complete_pct.ToString());
                                    break;
                                }
                                if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                {
                                    dbSQL.addParam("BN_Plan_complete_pct", maxplan_complete_pct- minplan_complete_pct);
                                    dbSQL.addParam("BN_act_complete_pct", maxact_complete_pct- minact_complete_pct);
                                }
                            }
                            dbSQL.doSQL(sUpBNBCWS.ToString());
                        }
                    }
                    #endregion

                    #endregion


                    //将计划分摊中的数据更新到作业周期表中(WBS)
                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='wbs' and plan_guid=@plan_guid  ");
                    dbSQL.addParam("period_guid", BCW[dsFeed.Tables[0].Rows[XH - 1]["Id"].ToString()].ToString());//上一期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBCWS = dbSQL.getDataSet(sBCWS.ToString());

                    sBCWS.Clear();
                    sBCWS.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='wbs'and plan_guid=@plan_guid ");
                    dbSQL.addParam("period_guid", BCW[MasterID].ToString());//本期的masterid
                    dbSQL.addParam("plan_guid", plan_guid);
                    dsBQ = dbSQL.getDataSet(sBCWS.ToString());
                    for (int i = 0; i < dsBQ.Tables[0].Rows.Count; i++)
                    {
                        WBSList.Remove(dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        StringBuilder sFirst = new StringBuilder();
                        sFirst.Clear();
                        sFirst.AppendLine("select Id,period_guid,periodstrat,periodend,type,task_guid,est_wt_pct,isnull(plan_complete_pct,0) as plan_complete_pct,rsrc_guid,rsrc_name,period_value,plan_guid,isnull(act_complete_pct,0) as act_complete_pct,isnull(act_value,0) as act_value,isnull(period_complete_pct,0) as period_complete_pct from PS_PLN_TaskBCWS where period_guid=@period_guid and type ='task' and plan_guid=@Firplan_guid ");
                        dbSQL.addParam("period_guid", BCW[dsRecord.Tables[0].Rows[0]["ID"].ToString()].ToString());//本周期第一个周期的masterid
                        dbSQL.addParam("Firplan_guid", plan_guid);
                        DataSet dsFirst = dbSQL.getDataSet(sFirst.ToString());

                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;
                        
                        //将所有周期中最大的查询出来
                        for (int k = 0; k < dsSYZQMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsSYZQMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                            }
                        }

                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_projwbsZQ set plan_complete_pct=@plan_complete_pct,");
                        sUpBCWS.AppendLine(" BY_act_complete_pct=@BY_act_complete_pct,BY_Plan_complete_pct=@BY_Plan_complete_pct,");
                        sUpBCWS.AppendLine(" act_complete_pct=@act_complete_pct,period_complete_pct=@period_complete_pct,period_Plan_complete_pct=@period_Plan_complete_pct ");
                        sUpBCWS.AppendLine("where masterid=@masterid and wbs_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", dsBQ.Tables[0].Rows[i]["task_guid"].ToString());
                        if (dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString().Equals("0"))
                            dbSQL.addParam("plan_complete_pct", maxplan_complete_pct);//计划完成百分比
                        else
                            dbSQL.addParam("plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//计划完成百分比
                        if (dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString().Equals("0"))
                            dbSQL.addParam("act_complete_pct", maxact_complete_pct);//实际完成百分比
                        else
                            dbSQL.addParam("act_complete_pct", dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString());//实际完成百分比
                        dbSQL.addParam("period_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期实际完成百分比
                        if (dsBCWS.Tables[0].Rows.Count > 0)//如果上期存在数据，则本期计划完成百分比等于累计计划完成百分比-上期累计计划完成百分比
                        {
                            string BYJHPct = "";
                            for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)//循环上期，比对作业的task_guid
                            {                               
                                if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                {
                                    double plan_complete_pct = 0;
                                    plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsBCWS.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                    dbSQL.addParam("period_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比
                                    BYJHPct = plan_complete_pct.ToString();
                                    break;
                                }
                                if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                {
                                    dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                                    BYJHPct = dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString();
                                }                                
                            }

                            if (FeedXH == 0 || FeedXH == 1)//说明这个周期落在第一个或者第二个，就要用本周期的数据减去上个周期得到本月计划%
                            {
                                for (int j = 0; j < dsBCWS.Tables[0].Rows.Count; j++)
                                {
                                    if (dsBCWS.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", BYJHPct);//本期计划完成百分比()实际就等于本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期实际完成百分比
                                        break;
                                    }
                                    if (j == dsBCWS.Tables[0].Rows.Count - 1)
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["Plan_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比()实际就等于本期实际完成百分比
                                    }
                                }
                            }
                            else if (FeedXH > 1)//就要用本期数据减去第一期数据得到本月计划%
                            {

                                for (int j = 0; j < dsBNBYMax.Tables[0].Rows.Count; j++)//循环本年本月最大最小数据
                                {
                                    if (dsBNBYMax.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))
                                    {
                                        double plan_complete_pct = 0;
                                        plan_complete_pct = double.Parse(dsBNBYMax.Tables[0].Rows[j]["act_complete_pct"].ToString()) - double.Parse(dsBNBYMax.Tables[0].Rows[j]["minact_complete_pct"].ToString());
                                        dbSQL.addParam("BY_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比

                                        double act_complete_pct = 0;
                                        act_complete_pct = double.Parse(dsBNBYMax.Tables[0].Rows[j]["plan_complete_pct"].ToString()) - double.Parse(dsBNBYMax.Tables[0].Rows[j]["minplan_complete_pct"].ToString());
                                        dbSQL.addParam("BY_act_complete_pct", act_complete_pct.ToString());//本期计划完成百分比
                                        break;
                                    }
                                    if (j == dsBNBYMax.Tables[0].Rows.Count - 1)
                                    {
                                        dbSQL.addParam("BY_Plan_complete_pct", BYJHPct);//本期计划完成百分比
                                        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                                    }
                                }
                                //for (int j = 0; j < dsFirst.Tables[0].Rows.Count; j++)
                                //{
                                //    if (dsFirst.Tables[0].Rows[j]["task_guid"].ToString().Equals(dsBQ.Tables[0].Rows[i]["task_guid"].ToString()))//第一个周期的task_guid等于本期的task_guid，则将本期的计划完成百分比减去第一期的计划完成百分比
                                //    {
                                //        double plan_complete_pct = 0;
                                //        plan_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString()) - double.Parse(dsFirst.Tables[0].Rows[j]["plan_complete_pct"].ToString());
                                //        dbSQL.addParam("BY_Plan_complete_pct", plan_complete_pct.ToString());//本期计划完成百分比


                                //        double act_complete_pct = 0;
                                //        act_complete_pct = double.Parse(dsBQ.Tables[0].Rows[i]["act_complete_pct"].ToString()) - double.Parse(dsFirst.Tables[0].Rows[j]["act_complete_pct"].ToString());
                                //        dbSQL.addParam("BY_act_complete_pct", act_complete_pct.ToString());//本期计划完成百分比
                                //        break;
                                //    }
                                //    if (j == dsFirst.Tables[0].Rows.Count - 1)
                                //    {
                                //        // dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                                //        dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                                //        dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                                //    }
                                //}
                            }
                        }
                        else//如果上期不存在数据，则本期计划完成就等于累计计划完成百分比
                        {
                            dbSQL.addParam("period_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                            dbSQL.addParam("BY_Plan_complete_pct", dsBQ.Tables[0].Rows[i]["plan_complete_pct"].ToString());//本期计划完成百分比
                            dbSQL.addParam("BY_act_complete_pct", dsBQ.Tables[0].Rows[i]["period_complete_pct"].ToString());//本期计划完成百分比
                        }
                        dbSQL.doSQL(sUpBCWS.ToString());
                    }

                    #region 本期作业外的wbs数据更新
                    for (int i = 0; i < WBSList.Count; i++)
                    {
                        double maxact_complete_pct = 0;
                        double maxplan_complete_pct = 0;
                        //将所有周期中最大的查询出来
                        for (int k = 0; k < dsSYZQMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsSYZQMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(WBSList[i].ToString()))
                            {
                                maxact_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                maxplan_complete_pct = double.Parse(dsSYZQMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                            }
                        }

                        double BNmaxact_complete_pct = 0;
                        double BNmaxplan_complete_pct = 0;
                        double BNminact_complete_pct = 0;
                        double BNminplan_complete_pct = 0;
                        //将本年的最大最小值查询出来
                        for (int k = 0; k < dsBNMax.Tables[0].Rows.Count; k++)
                        {
                            if (dsBNMax.Tables[0].Rows[k]["task_guid"].ToString().Equals(WBSList[i].ToString()))
                            {
                                BNmaxact_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["act_complete_pct"].ToString());
                                BNmaxplan_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["plan_complete_pct"].ToString());
                                BNminact_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["minact_complete_pct"].ToString());
                                BNminplan_complete_pct = double.Parse(dsBNMax.Tables[0].Rows[k]["minplan_complete_pct"].ToString());
                            }
                        }

                        StringBuilder sUpBCWS = new StringBuilder();
                        sUpBCWS.Clear();
                        sUpBCWS.AppendLine("update pln_projwbsZQ set plan_complete_pct=@plan_complete_pct,");
                        sUpBCWS.AppendLine(" BY_act_complete_pct=0,BY_Plan_complete_pct=0,");
                        sUpBCWS.AppendLine(" act_complete_pct=@act_complete_pct,BN_Plan_complete_pct=@BN_Plan_complete_pct,BN_act_complete_pct=@BN_act_complete_pct ");
                        sUpBCWS.AppendLine("where masterid=@masterid and wbs_guid=@task_guid and plan_guid=@plan_guid");
                        dbSQL.addParam("masterid", MasterID);
                        dbSQL.addParam("plan_guid", plan_guid);
                        dbSQL.addParam("task_guid", WBSList[i].ToString());

                            dbSQL.addParam("plan_complete_pct", maxplan_complete_pct);//计划完成百分比

                            dbSQL.addParam("act_complete_pct", maxact_complete_pct);//实际完成百分比

                        dbSQL.addParam("BN_Plan_complete_pct", BNmaxplan_complete_pct- BNminplan_complete_pct);//本年计划完成%
                        dbSQL.addParam("BN_act_complete_pct", BNmaxact_complete_pct- BNminact_complete_pct);//本年实际完成%
                        dbSQL.doSQL(sUpBCWS.ToString());
                    }                    
                    #endregion
                }
                dbSQL.commitTran();
            }
            catch (Exception ex)
            {
                dbSQL.rollback();
                throw (ex);
            }
            return "ok";
        }
        /// <summary>
        /// 插入wbs
        /// </summary>
        /// <param name="MasterID"></param>
        /// <param name="plan_guid"></param>
        public  void InsertWBS(string MasterID, string plan_guid)//插入wbs
        {
            StringBuilder sInsPL = new StringBuilder();
            sInsPL.Clear();
            sInsPL.AppendLine(sWBS.ToString());
            dbSQL.addParam("plan_guid", plan_guid);
            dbSQL.addParam("MasterId", MasterID);
            dbSQL.doSQL(sInsPL.ToString());
        }

        /// <summary>
        /// 插入作业
        /// </summary>
        /// <param name="MasterID"></param>
        /// <param name="plan_guid"></param>
        public  void InsertZY(string MasterID, string plan_guid)//插入作业
        {
            StringBuilder sInsPL = new StringBuilder();
            sInsPL.Clear();
            sInsPL.AppendLine(sZY.ToString());
            dbSQL.addParam("plan_guid", plan_guid);
            dbSQL.addParam("MasterId", MasterID);
            dbSQL.doSQL(sInsPL.ToString());
        }


       static string InsProc = @"insert into PLN_TaskprocZQ(proj_id,wbs_id,task_id,
                        task_guid,seq_num,CompleteOrNot,proc_name,
                        proc_descri,est_wt,complete_pct,act_end_date,
                        SysOrNot,target_end_date_lag,expect_end_date_lag,rsrc_id,temp_id,
                        update_date,p3ec_proc_id,p3ec_flag,
                        proc_guid,proj_guid,plan_guid,plan_id,
                        wbs_guid,rsrc_guid,temp_guid,est_wt_pct,
                        keyword,formid,update_user,create_date,
                        create_user,delete_session_id,delete_date,
                        target_end_date,proc_code,PLN_target_start_date,PLN_target_end_date,
                         PLN_act_end_date,PLN_act_start_date,MasterId)
						 values(@proj_id,@wbs_id,@task_id,
                        @task_guid,@seq_num,@CompleteOrNot,@proc_name,
                        @proc_descri,@est_wt,@complete_pct,@act_end_date,
                        @SysOrNot,@target_end_date_lag,@expect_end_date_lag,@rsrc_id,@temp_id,
                        @update_date,@p3ec_proc_id,@p3ec_flag,
                        @proc_guid,@proj_guid,@plan_guid,@plan_id,
                        @wbs_guid,@rsrc_guid,@temp_guid,@est_wt_pct,
                        @keyword,@formid,@update_user,@create_date,
                        @create_user,@delete_session_id,@delete_date,
                        @target_end_date,@proc_code,@PLN_target_start_date,@PLN_target_end_date,
                         @PLN_act_end_date,@PLN_act_start_date,@MasterId)";

        static string InsSub = @"insert into PS_PLN_TaskProc_SubZQ( 
                             ProcSub_guid,ProcSub_Name,proj_guid,plan_guid,
                             wbs_guid,task_guid,proc_guid,MasterId,
                            seq_num,est_wt,
                            est_wt_pct,complete_pct,target_end_date,
                            act_end_date,temp_guid,remark,RegDate,
                            RegHumName,RegHumId,UpdHumId,
                            UpdHuman,UpdDate,CheckDate,SubState,
                            CompleteDate,ProcSub_Code) 
                             values (@ProcSub_guid,@ProcSub_Name,@proj_guid,@plan_guid,
                             @wbs_guid,@task_guid,@proc_guid,@MasterId,
                            @seq_num,@est_wt,
                            @est_wt_pct,@complete_pct,@target_end_date,
                            @act_end_date,@temp_guid,@remark,@RegDate,
                            @RegHumName,@RegHumId,@UpdHumId,
                            @UpdHuman,@UpdDate,@CheckDate,@SubState,
                            @CompleteDate,@ProcSub_Code)";

        static string Ins_Nps = @"insert into NPS_BOQZQ(ID,MasterId,Code,Title,TableName,BizAreaId,Sequ,Status,
                             RegHumId,RegHumName,RegDate,RegPosiId,RegDeptId,
                             RecycleHumId,UpdHumId,UpdHumName,UpdDate,ApprHumId,ApprHumName,
                             ApprDate,Remark,OwnProjName,EpsProjCode,EpsProjName,
                             FID,Chapter,ListingCode,ListingName,ListingPrice,ListingNum,
                             Amount,PlanNum,PlanPrice,PlanSatrt,PlanEnd,S_ISBN,Ndljtz,act_startdate,act_enddate,KGLJGCL,KGLJJE,BYJHGCL,BYJHGCJE,BNJHGCJE,BYSJGCL,BYSJJE,BNJHGCL ) 
                             values (@ID,@MasterId,@Code,@Title,@TableName,@BizAreaId,@Sequ,@Status,
							 @RegHumId,@RegHumName,@RegDate,@RegPosiId,@RegDeptId,
							 @RecycleHumId,@UpdHumId,@UpdHumName,@UpdDate,@ApprHumId,@ApprHumName,@ApprDate,@Remark,@OwnProjName,@EpsProjCode,@EpsProjName,
                             @FID,@Chapter,@ListingCode,@ListingName,@ListingPrice,@ListingNum,
							 @Amount,@PlanNum,@PlanPrice,@PlanSatrt,@PlanEnd,@S_ISBN,@Ndljtz,@act_startdate,@act_enddate,@KGLJGCL,@KGLJJE,@BYJHGCL,@BYJHGCJE,@BNJHGCJE,@BYSJGCL,@BYSJJE,@BNJHGCL )";

        public static void InsertTaskprocZQ(string currentfeedback, string currentplan, string currentperiod, string currentrsrc)
        {
            DateTime period_begindate = new DateTime();
            DateTime period_enddate = new DateTime();
            StringBuilder sFeedBackSQL = new StringBuilder();
            sFeedBackSQL.Clear();
            sFeedBackSQL.AppendLine("Select period_begindate,period_enddate From  PS_PLN_FeedBackRecord where ");
            sFeedBackSQL.AppendLine("plan_guid=@plan_guid and period_guid=@period_guid and RegHumId=@RegHumId ");
            dbSQL.addParam("plan_guid", Guid.Parse(currentplan));
            dbSQL.addParam("period_guid", Guid.Parse(currentperiod));
            dbSQL.addParam("RegHumId", Guid.Parse(currentrsrc));
            DataSet dsFeed = dbSQL.getDataSet(sFeedBackSQL.ToString());
            for (int i = 0; i < dsFeed.Tables[0].Rows.Count; i++)
            {
                period_begindate = Convert.ToDateTime(dsFeed.Tables[0].Rows[0]["period_begindate"].ToString());
                period_enddate = Convert.ToDateTime(dsFeed.Tables[0].Rows[0]["period_enddate"].ToString());
            }
            dbSQL.beginTran();
            try
            {
                string sSQL = "Select masterid,task_guid From V_PS_PLN_FeedBackTask VPPFB Where  ";
                sSQL += "1=1  and ( VPPFB.MasterId='" + currentfeedback + "')  ";
                sSQL += "and (1=1 ) and rsrc_guid is not null Order By  type,VPPFB.task_code,VPPFB.Sequ ASC ";
                sFeedBackSQL.Clear();
                sFeedBackSQL.AppendLine(sSQL);
                dsFeed = dbSQL.getDataSet(sFeedBackSQL.ToString());
                for (int i = 0; i < dsFeed.Tables[0].Rows.Count; i++)
                {

                    string masterid = dsFeed.Tables[0].Rows[i]["masterid"].ToString();
                    string task_guid = dsFeed.Tables[0].Rows[i]["task_guid"].ToString();

                    string sSQL1 = "select proc_id,proj_id,wbs_id,task_id,task_guid,seq_num,CompleteOrNot,proc_name,proc_descri,est_wt,complete_pct,act_end_date,SysOrNot,target_end_date_lag,expect_end_date_lag,rsrc_id,temp_id,";
                    sSQL1 += "update_date,p3ec_proc_id,p3ec_flag,proc_guid,proj_guid,plan_guid,plan_id,wbs_guid,rsrc_guid,temp_guid,est_wt_pct,keyword,formid,update_user,create_date,create_user,delete_session_id,delete_date,";
                    sSQL1 += "target_end_date,proc_code,PLN_target_start_date,PLN_target_end_date,PLN_act_end_date,PLN_act_start_date from  PLN_TaskProc  where task_guid = '" + task_guid + "'  order by seq_num";

                    sFeedBackSQL.Clear();
                    sFeedBackSQL.AppendLine(sSQL1);
                    DataSet dsTaskProc = dbSQL.getDataSet(sFeedBackSQL.ToString());
                    for (int j = 0; j < dsTaskProc.Tables[0].Rows.Count; j++)
                    {
                        float complete_pct = 0;
                        string sSQL3 = "select sum(complete_pct*est_wt_pct) as complete_pct from PS_PLN_TaskProc_Sub where proc_guid='" + dsTaskProc.Tables[0].Rows[j]["proc_guid"].ToString() + "' group by proc_guid";
                        sFeedBackSQL.Clear();
                        sFeedBackSQL.AppendLine(sSQL3);
                        DataSet dsSub = dbSQL.getDataSet(sFeedBackSQL.ToString());
                        if (dsSub.Tables[0].Rows.Count > 0)
                        {
                            if (dsSub.Tables[0].Rows[0]["complete_pct"].ToString().Trim().Equals(""))
                            {
                                complete_pct = 0;
                            }
                            else
                                complete_pct = float.Parse(dsSub.Tables[0].Rows[0]["complete_pct"].ToString());
                        }
                        else if (dsSub.Tables[0].Rows.Count == 0)
                        {
                            if (dsSub.Tables[0].Rows[j]["PLN_act_end_date"].ToString().Trim().Equals("") || string.IsNullOrEmpty(dsSub.Tables[0].Rows[j]["PLN_act_end_date"].ToString()))
                            {
                                complete_pct = 0;
                            }
                            else
                            {
                                complete_pct = 100;
                            }
                        }

                        string CheckDate = "";
                        string CompleteDate = "";
                        sSQL3 = "";
                        sSQL3 = "select min(CheckDate) as CheckDate,max(CompleteDate) as CompleteDate  from PS_PLN_TaskProc_Sub where proc_guid='" + dsTaskProc.Tables[0].Rows[j]["proc_guid"].ToString() + "' group by proc_guid";
                        sFeedBackSQL.Clear();
                        sFeedBackSQL.AppendLine(sSQL3);
                        DataSet dsTaskProc_Sub = dbSQL.getDataSet(sFeedBackSQL.ToString());
                        if (dsTaskProc_Sub.Tables[0].Rows.Count > 0)
                        {
                            CheckDate = dsTaskProc_Sub.Tables[0].Rows[0]["CheckDate"].ToString();
                            CompleteDate = dsTaskProc_Sub.Tables[0].Rows[0]["CompleteDate"].ToString();
                        }

                        InsGJ(dsTaskProc.Tables[0].Rows[j], complete_pct, masterid);//插入构件

                        sSQL3 = "";
                        sSQL3 = "select ProcSub_guid,ProcSub_Name,proj_guid,plan_guid,wbs_guid,task_guid,proc_guid,seq_num,est_wt,est_wt_pct,complete_pct,";
                        sSQL3 += "      temp_guid,remark,RegDate,RegHumName,RegHumId,UpdHumId,UpdHuman,UpdDate,CheckDate,SubState,CompleteDate,ProcSub_Code,target_end_date,act_end_date";
                        sSQL3 += " from PS_PLN_TaskProc_Sub ";
                        sSQL3 += "where proc_guid='" + dsTaskProc.Tables[0].Rows[j]["proc_guid"].ToString() + "'";
                        sFeedBackSQL.Clear();
                        sFeedBackSQL.AppendLine(sSQL3);
                        DataSet ds_Sub = dbSQL.getDataSet(sFeedBackSQL.ToString());
                        for (int k = 0; k < ds_Sub.Tables[0].Rows.Count; k++)
                        {
                            string WCBFB = "0";
                            if (ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString().Equals("1900-01-01") || string.IsNullOrEmpty(ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString()))
                            {
                                WCBFB = "0";
                            }
                            else
                            {
                                WCBFB = "100";
                            }
                            string SubState = "";
                            if ((ds_Sub.Tables[0].Rows[k]["CheckDate"].ToString().Equals("1900-01-01") || string.IsNullOrEmpty(ds_Sub.Tables[0].Rows[k]["CheckDate"].ToString()))
                                && (ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString().Equals("1900-01-01") || string.IsNullOrEmpty(ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString())))
                            {
                                SubState = "未开始";
                            }
                            else if ((!ds_Sub.Tables[0].Rows[k]["CheckDate"].ToString().Equals("1900-01-01") && !string.IsNullOrEmpty(ds_Sub.Tables[0].Rows[k]["CheckDate"].ToString()))
                                && (ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString().Equals("1900-01-01") || string.IsNullOrEmpty(ds_Sub.Tables[0].Rows[k]["CompleteDate"].ToString())))
                            {
                                SubState = "已开始";
                            }
                            else
                            {
                                SubState = "已完成";
                            }

                            Ins_Sub(ds_Sub.Tables[0].Rows[k], SubState, masterid, WCBFB);
                        }


                        string sBOQ = "";
                        sBOQ = " select ID,Code,Title,TableName,BizAreaId,Sequ,Status,";
                        sBOQ += " RegHumId,RegHumName,RegDate,RegPosiId,RegDeptId,EpsProjId,";
                        sBOQ += " RecycleHumId,UpdHumId,UpdHumName,UpdDate,ApprHumId,ApprHumName,";
                        sBOQ += " ApprDate,Remark,OwnProjId,OwnProjName,EpsProjCode,EpsProjName,";
                        sBOQ += " FID,Chapter,ListingCode,ListingName,ListingPrice,ListingNum,";
                        sBOQ += " Amount,PlanNum,PlanPrice,PlanSatrt,PlanEnd,S_ISBN,Ndljtz from NPS_BOQ";
                        sBOQ += " where FID='" + dsTaskProc.Tables[0].Rows[j]["proc_guid"].ToString() + "' ";
                        DataSet NPS_BOQZQ = dbSQL.getDataSet(sBOQ.ToString());
                        for (int h = 0; h < NPS_BOQZQ.Tables[0].Rows.Count; h++)
                        {
                            double KGLJGCL = 0;
                            double KGLJJE = 0;
                            if ((!dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString().Equals("1900-01-01") && !string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString()))
                                && (dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString().Equals("1900-01-01") || string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString())))
                            {
                                KGLJGCL = float.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingNum"].ToString()) * (float.Parse(dsTaskProc.Tables[0].Rows[j]["complete_pct"].ToString()) / 100);
                                KGLJJE = float.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingPrice"].ToString()) * (float.Parse(dsTaskProc.Tables[0].Rows[j]["complete_pct"].ToString()) / 100);
                            }
                            else if ((!dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString().Equals("1900-01-01") && !string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString()))
                                && (!dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString().Equals("1900-01-01") && !string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString())))
                            {
                                KGLJGCL = float.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingNum"].ToString());
                                KGLJJE = float.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingPrice"].ToString());
                            }

                            double BYJHGCL = 0; //本月计划实物工程量
                            double BYJHGCJE = 0;//本月计划实物工程量（元）
                            double BNJHGCJE = 0; //本年计划实物工程量（元）
                            if (!string.IsNullOrEmpty(NPS_BOQZQ.Tables[0].Rows[h]["PlanSatrt"].ToString()))
                            {
                                if (DateTime.Parse(NPS_BOQZQ.Tables[0].Rows[h]["PlanSatrt"].ToString()).Month.Equals(period_begindate.Month))
                                {
                                    BYJHGCL = double.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingNum"].ToString());
                                    BYJHGCJE = double.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingPrice"].ToString());

                                }
                            }
                            if (!string.IsNullOrEmpty(NPS_BOQZQ.Tables[0].Rows[h]["PlanEnd"].ToString()))
                            {
                                if (DateTime.Parse(NPS_BOQZQ.Tables[0].Rows[h]["PlanEnd"].ToString()).Year.Equals(period_enddate.Year))
                                {
                                    BNJHGCJE = double.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingPrice"].ToString());
                                }
                            }

                            double SQKGLJGCL = 0;//上期开工累计工程量   
                            double SQKGLJJE = 0;//上期开工累计金额
                                                //计算本月实际实物工程量
                                                //根据PLAN_GUID和周期后一个日期字段查询PS_PLN_FeedBackRecord表的ID字段，确定哪个周期，然后根据ID对应MasterId,找PS_PLN_FeedBackRecord_Task表，
                                                //查询出task_guid，根据task_guid和proc_guid字段，查询出NPS_BOQZQ的开工累计实物工程量。
                            if (string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString()))
                                CompleteDate = "1900-01-01";
                            if (string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString()))
                                CheckDate = "1900-01-01";

                            string sSQL4 = "select ID,plan_guid from PS_PLN_FeedBackRecord where plan_guid='" + Guid.Parse(currentplan) + "' and period_enddate=(select max(period_enddate) from ";
                            sSQL4 += "PS_PLN_FeedBackRecord where month(period_enddate)=month('" + period_enddate + "')-1) ";
                            DataSet FeedBackRecord = dbSQL.getDataSet(sSQL4.ToString());
                            if (FeedBackRecord.Tables[0].Rows.Count > 0)
                            {
                                string master = FeedBackRecord.Tables[0].Rows[0]["ID"].ToString();
                                string sSQL6 = "select KGLJGCL,KGLJJE from NPS_BOQZQ where masterid='" + master + "' and id='" + NPS_BOQZQ.Tables[0].Rows[h]["ID"].ToString() + "'";
                                DataSet SQKGLJGCLData = dbSQL.getDataSet(sSQL6.ToString());
                                if (SQKGLJGCLData.Tables[0].Rows.Count > 0)
                                {
                                    SQKGLJGCL = float.Parse(SQKGLJGCLData.Tables[0].Rows[0]["KGLJGCL"].ToString());
                                    SQKGLJJE = float.Parse(SQKGLJGCLData.Tables[0].Rows[0]["KGLJJE"].ToString());
                                }
                            }

                            double BYSJGCL = 0;
                            double BYSJJE = 0;
                            BYSJGCL = KGLJGCL - SQKGLJGCL;//本月实际实物工程量
                            BYSJJE = KGLJJE - SQKGLJJE;//本月实际实物工程量（元）

                            if (string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString()) && string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_start_date"].ToString()))//无实际开始时间，无实际完成时间
                            {
                                BYSJGCL = 0;
                                BYSJJE = 0;
                            }
                            if (!string.IsNullOrEmpty(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString()))
                            {
                                if (!DateTime.Parse(dsTaskProc.Tables[0].Rows[j]["PLN_act_end_date"].ToString()).Month.Equals(period_enddate.Month))//实际完成时间与周期后一个判断是否再同一个月
                                {
                                    BYSJGCL = 0;
                                    BYSJJE = 0;
                                }
                            }

                            //计算本年计划工程量
                            double BNJHGCL = 0;
                            if (!string.IsNullOrEmpty(NPS_BOQZQ.Tables[0].Rows[h]["Planend"].ToString()))
                            {
                                if (DateTime.Parse(NPS_BOQZQ.Tables[0].Rows[h]["Planend"].ToString()).Year.Equals(period_enddate.Year))
                                {
                                    BNJHGCL = float.Parse(NPS_BOQZQ.Tables[0].Rows[h]["ListingNum"].ToString());
                                }
                            }
                            InsNps(NPS_BOQZQ.Tables[0].Rows[h], masterid, KGLJGCL, KGLJJE, BYJHGCL, BYJHGCJE, BNJHGCJE, BYSJGCL, BYSJJE, BNJHGCL);
                        }

                    }
                    
                }
                    dbSQL.commitTran();
            }
            catch (Exception ex)
            {
                dbSQL.rollback();
                throw (ex);
            }       
        }


        //插入工程量清单
        public static void InsNps(DataRow NpsBOQ,string MasterId,double KGLJGCL, double KGLJJE, double BYJHGCL, double BYJHGCJE, double BNJHGCJE, double BYSJGCL, double BYSJJE, double BNJHGCL)
        {
            StringBuilder sFeedBackSQL = new StringBuilder();
            sFeedBackSQL.Clear();
            sFeedBackSQL.AppendLine(Ins_Nps);
            dbSQL.addParam("ID", NpsBOQ["ID"].ToString());

            dbSQL.addParam("MasterId", MasterId);
            dbSQL.addParam("Code", NpsBOQ["Code"].ToString());
            dbSQL.addParam("Title", NpsBOQ["Title"].ToString());
            dbSQL.addParam("TableName", NpsBOQ["TableName"].ToString());
            dbSQL.addParam("BizAreaId", NpsBOQ["BizAreaId"].ToString());
            dbSQL.addParam("Sequ", NpsBOQ["Sequ"].ToString());
            dbSQL.addParam("Status", NpsBOQ["Status"].ToString());
            dbSQL.addParam("RegHumId", NpsBOQ["RegHumId"].ToString());
            dbSQL.addParam("RegHumName", NpsBOQ["RegHumName"].ToString());
            dbSQL.addParam("RegDate", NpsBOQ["RegDate"].ToString());
            dbSQL.addParam("RegPosiId", NpsBOQ["RegPosiId"].ToString());
            dbSQL.addParam("RegDeptId", NpsBOQ["RegDeptId"].ToString());
            dbSQL.addParam("RecycleHumId", NpsBOQ["RecycleHumId"].ToString());
            dbSQL.addParam("UpdHumId", NpsBOQ["UpdHumId"].ToString());
            dbSQL.addParam("UpdHumName", NpsBOQ["UpdHumName"].ToString());
            dbSQL.addParam("UpdDate", NpsBOQ["UpdDate"].ToString());
            dbSQL.addParam("ApprHumId", NpsBOQ["ApprHumId"].ToString());

            dbSQL.addParam("ApprHumName", NpsBOQ["ApprHumName"].ToString()); 
            dbSQL.addParam("ApprDate", NpsBOQ["ApprDate"].ToString());
            dbSQL.addParam("Remark", NpsBOQ["Remark"].ToString());
            dbSQL.addParam("OwnProjName", NpsBOQ["OwnProjName"].ToString());
            dbSQL.addParam("EpsProjCode", NpsBOQ["EpsProjCode"].ToString());


            dbSQL.addParam("EpsProjName", NpsBOQ["EpsProjName"].ToString()); 
            dbSQL.addParam("FID", NpsBOQ["FID"].ToString());
            dbSQL.addParam("Chapter", NpsBOQ["Chapter"].ToString());

            dbSQL.addParam("ListingCode", NpsBOQ["ListingCode"].ToString());
            dbSQL.addParam("ListingName", NpsBOQ["ListingName"].ToString());
            dbSQL.addParam("ListingPrice", NpsBOQ["ListingPrice"].ToString());
            dbSQL.addParam("ListingNum", NpsBOQ["ListingNum"].ToString()); 
            dbSQL.addParam("Amount", NpsBOQ["Amount"].ToString()); 
            dbSQL.addParam("PlanNum", NpsBOQ["PlanNum"].ToString()); 
            dbSQL.addParam("PlanPrice", NpsBOQ["PlanPrice"].ToString());
            dbSQL.addParam("PlanSatrt", NpsBOQ["PlanSatrt"].ToString());
            dbSQL.addParam("PlanEnd", NpsBOQ["PlanEnd"].ToString());
            dbSQL.addParam("S_ISBN", NpsBOQ["S_ISBN"].ToString());
            dbSQL.addParam("Ndljtz", NpsBOQ["Ndljtz"].ToString());

            dbSQL.addParam("act_startdate", NpsBOQ["act_startdate"].ToString());
            dbSQL.addParam("act_enddate", NpsBOQ["act_enddate"].ToString());
            dbSQL.addParam("KGLJGCL", KGLJGCL);
            dbSQL.addParam("KGLJJE", KGLJJE); 
            dbSQL.addParam("BYJHGCL", BYJHGCL);
            dbSQL.addParam("BYJHGCJE", BYJHGCJE);
            dbSQL.addParam("BNJHGCJE", BNJHGCJE);
            dbSQL.addParam("BYSJGCL", BYSJGCL);
            dbSQL.addParam("BYSJJE", BYSJJE);
            dbSQL.addParam("BNJHGCL", BNJHGCL);
        }

        public static void Ins_Sub(DataRow ds_Sub,string SubState,string masterid,string WCBFB)//插入工序
        {
            StringBuilder sFeedBackSQL = new StringBuilder();
            sFeedBackSQL.Clear();
            sFeedBackSQL.AppendLine(InsSub);
            dbSQL.addParam("ProcSub_guid", ds_Sub["ProcSub_guid"].ToString());
            dbSQL.addParam("ProcSub_Name", ds_Sub["ProcSub_Name"].ToString());
            dbSQL.addParam("proj_guid", ds_Sub["proj_guid"].ToString());
            dbSQL.addParam("plan_guid", ds_Sub["plan_guid"].ToString());
            dbSQL.addParam("wbs_guid", ds_Sub["wbs_guid"].ToString()); 
            dbSQL.addParam("task_guid", ds_Sub["task_guid"].ToString());
            dbSQL.addParam("proc_guid", ds_Sub["proc_guid"].ToString());
            dbSQL.addParam("MasterId", masterid);
            dbSQL.addParam("seq_num", ds_Sub["seq_num"].ToString());
            dbSQL.addParam("est_wt", ds_Sub["est_wt"].ToString());
            dbSQL.addParam("est_wt_pct", ds_Sub["est_wt_pct"].ToString());
            dbSQL.addParam("complete_pct", WCBFB);
            dbSQL.addParam("target_end_date", ds_Sub["target_end_date"].ToString());
            dbSQL.addParam("temp_guid", ds_Sub["temp_guid"].ToString());
            dbSQL.addParam("remark", ds_Sub["remark"].ToString());
            dbSQL.addParam("RegDate", ds_Sub["RegDate"].ToString());
            dbSQL.addParam("RegHumName", ds_Sub["RegHumName"].ToString());
            dbSQL.addParam("RegHumId", ds_Sub["RegHumId"].ToString());
            dbSQL.addParam("UpdHumId", ds_Sub["UpdHumId"].ToString());
            dbSQL.addParam("UpdHuman", ds_Sub["UpdHuman"].ToString());
            dbSQL.addParam("UpdDate", ds_Sub["UpdDate"].ToString());
            if (!string.IsNullOrEmpty(ds_Sub["CheckDate"].ToString()))
                dbSQL.addParam("CheckDate", ds_Sub["CheckDate"].ToString());
            else
                dbSQL.addParam("CheckDate", DBNull.Value);
            dbSQL.addParam("SubState", SubState);
            if(!string.IsNullOrEmpty(ds_Sub["CompleteDate"].ToString()))
            dbSQL.addParam("CompleteDate", ds_Sub["CompleteDate"].ToString());
            else
                dbSQL.addParam("CompleteDate", DBNull.Value);
            dbSQL.addParam("ProcSub_Code", ds_Sub["ProcSub_Code"].ToString());
        }

        public static void InsGJ(DataRow dsTaskProc,float complete_pct,string masterid)//插入构件
        {
            StringBuilder sFeedBackSQL = new StringBuilder();
            sFeedBackSQL.Clear();
            sFeedBackSQL.AppendLine(InsProc);
            dbSQL.addParam("proj_id", dsTaskProc["proj_id"].ToString());
            dbSQL.addParam("wbs_id", dsTaskProc["wbs_id"].ToString());
            dbSQL.addParam("task_id", dsTaskProc["task_id"].ToString());
            dbSQL.addParam("task_guid", dsTaskProc["task_guid"].ToString());
            dbSQL.addParam("seq_num", dsTaskProc["seq_num"].ToString());
            dbSQL.addParam("CompleteOrNot", dsTaskProc["CompleteOrNot"].ToString());
            dbSQL.addParam("proc_name", dsTaskProc["proc_name"].ToString());
            dbSQL.addParam("proc_descri", dsTaskProc["proc_descri"].ToString());
            dbSQL.addParam("est_wt", dsTaskProc["est_wt"].ToString());
            dbSQL.addParam("complete_pct", complete_pct);
            dbSQL.addParam("act_end_date", dsTaskProc["act_end_date"].ToString());
            dbSQL.addParam("SysOrNot", dsTaskProc["SysOrNot"].ToString());
            dbSQL.addParam("target_end_date_lag", dsTaskProc["target_end_date_lag"].ToString());
            dbSQL.addParam("expect_end_date_lag", dsTaskProc["expect_end_date_lag"].ToString());
            dbSQL.addParam("rsrc_id", dsTaskProc["rsrc_id"].ToString());
            dbSQL.addParam("temp_id", dsTaskProc["temp_id"].ToString());
            dbSQL.addParam("update_date", dsTaskProc["update_date"].ToString());
            dbSQL.addParam("p3ec_proc_id", dsTaskProc["p3ec_proc_id"].ToString());
            dbSQL.addParam("p3ec_flag", dsTaskProc["p3ec_flag"].ToString());
            dbSQL.addParam("proc_guid", dsTaskProc["proc_guid"].ToString());
            dbSQL.addParam("proj_guid", dsTaskProc["proj_guid"].ToString());
            dbSQL.addParam("plan_guid", dsTaskProc["plan_guid"].ToString());
            dbSQL.addParam("plan_id", dsTaskProc["plan_id"].ToString());
            dbSQL.addParam("wbs_guid", dsTaskProc["wbs_guid"].ToString());
            dbSQL.addParam("rsrc_guid", dsTaskProc["rsrc_guid"].ToString());
            dbSQL.addParam("temp_guid", dsTaskProc["temp_guid"].ToString());
            dbSQL.addParam("est_wt_pct", dsTaskProc["est_wt_pct"].ToString());
            dbSQL.addParam("keyword", dsTaskProc["keyword"].ToString());
            dbSQL.addParam("formid", dsTaskProc["formid"].ToString());
            dbSQL.addParam("update_user", dsTaskProc["update_user"].ToString());
            dbSQL.addParam("create_date", dsTaskProc["create_date"].ToString());
            dbSQL.addParam("create_user", dsTaskProc["create_user"].ToString());
            dbSQL.addParam("delete_session_id", dsTaskProc["delete_session_id"].ToString());
            dbSQL.addParam("delete_date", dsTaskProc["delete_date"].ToString());
            dbSQL.addParam("target_end_date", dsTaskProc["target_end_date"].ToString());
            dbSQL.addParam("proc_code", dsTaskProc["proc_code"].ToString());
            dbSQL.addParam("PLN_target_start_date", dsTaskProc["PLN_target_start_date"].ToString());
            dbSQL.addParam("PLN_target_end_date", dsTaskProc["PLN_target_end_date"].ToString());
            dbSQL.addParam("PLN_act_end_date", dsTaskProc["PLN_act_end_date"].ToString());
            dbSQL.addParam("PLN_act_start_date", dsTaskProc["PLN_act_start_date"].ToString());
            dbSQL.addParam("masterid", masterid);
        }


        public void UpdateSubZQ(string masterid, string proc_guid, string currentplan, string currentperiod, string currentrsrc, Boolean SFGXSJ = false)
        {
            DateTime period_begindate = new DateTime();
            DateTime period_enddate = new DateTime();
            double SQKGLJGCL = 0;//上期开工累计工程量   
            double SQKGLJJE = 0;//上期开工累计金额
            double KGLJGCL = 0;//开工累计实物工程量
            double KGLJGCJE = 0; //开工累计实物工程量（元）
            double BYSJGCL = 0;//本月实际实物工程量
            double BYSJGCJE = 0;//本月实际实物工程量（元）
            string sFeedBackSQL = "Select period_begindate,period_enddate From  PS_PLN_FeedBackRecord where ";//当前周期的开始-结束时间
            sFeedBackSQL += "plan_guid='" + Guid.Parse(currentplan) + "' and period_guid='" + Guid.Parse(currentperiod) + "' and RegHumId='" + Guid.Parse(currentrsrc) + "' ";
            DataSet FeedBackData = dbSQL.getDataSet(sFeedBackSQL.ToString());
            if (FeedBackData.Tables[0].Rows.Count > 0)
            {
                period_begindate = Convert.ToDateTime(FeedBackData.Tables[0].Rows[0]["period_begindate"].ToString());
                period_enddate = Convert.ToDateTime(FeedBackData.Tables[0].Rows[0]["period_enddate"].ToString());
            }

        }

        static void Main(string[] args)
        {
            InsertTaskprocZQ("75E0FD24-EA06-4966-B82E-AA4C12B0B66F", "E223174F-C611-D125-A275-B3CA7D70EB87", "13FCF46D-466E-4DDE-A44C-54EE3AAB5F7C", "037E113C-0A4D-4AFF-A208-B1E6A281A44F");
        }
    }

    public class SqlDataBase
    {
        private const int MaxPool = 512;//最大连接数
        private const int MinPool = 5;//最小连接数
        private const bool Asyn_Process = true;//设置异步访问数据库
                                               //在单个连接上得到和管理多个、仅向前引用和只读的结果集(ADO.NET2.0)
        private const bool Mars = true;
        private const int Conn_Timeout = 5000;//设置连接等待时间
        private const int Conn_Lifetime = 15000;//设置连接的生命周期
        private SqlConnection con = null;//连接对象
        private SqlTransaction dbTran = null;
        public Dictionary<string, object> paramList = new Dictionary<string, object>();

        public SqlDataBase()
        {
            //server=47.101.200.39;database=JJPowerPMDBTest;uid=sa;pwd=Power3506
            //string sConnectionString = @"server=47.101.200.39;database=JJPowerPMDBTest;uid=sa;pwd=Power3506;"
            string sConnectionString = @"server=47.101.200.39;database=JJPowerPMDBTest;uid=sa;pwd=Power3506;"
                                     + "Max Pool Size=" + MaxPool + ";"
                                     + "Min Pool Size=" + MinPool + ";"
                                     + "Connect Timeout=" + Conn_Timeout + ";"
                                     + "Connection Lifetime=" + Conn_Lifetime + ";"
                                     + "Asynchronous Processing=" + Asyn_Process + ";";
            con = new SqlConnection(sConnectionString);
        }

        public SqlDataBase(string sLJCS)
        {
            string sConnectionString = sLJCS + ";";
            sConnectionString += @"Max Pool Size=" + MaxPool + ";"
                               + "Min Pool Size=" + MinPool + ";"
                               + "Connect Timeout=" + Conn_Timeout + ";"
                               + "Connection Lifetime=" + Conn_Lifetime + ";"
                               + "Asynchronous Processing=" + Asyn_Process + ";";
            con = new SqlConnection(sConnectionString);
        }

        public void beginTran()
        {
            if (con.State == ConnectionState.Closed)
                con.Open();
            dbTran = con.BeginTransaction();
        }

        public void commitTran()
        {
            try
            {
                dbTran.Commit();
            }
            catch
            {
                dbTran.Rollback();
            }
            finally
            {
                con.Close();
            }
        }

        public void rollback()
        {
            dbTran.Rollback();
            con.Close();
        }

        public DataSet getDataSet(string sSQL)
        {
            if (con.State == ConnectionState.Closed)
                con.Open();
            SqlCommand cmd = new SqlCommand(sSQL, con);
            cmd.Parameters.Clear();
            foreach (KeyValuePair<string, object> dic in paramList)
                cmd.Parameters.AddWithValue(dic.Key, dic.Value);
            if (dbTran != null)
                cmd.Transaction = dbTran;
            SqlDataAdapter sda = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            sda.Fill(ds);
            clearParam();
            if (dbTran == null)
                if (dbTran == null)
                    con.Close();
            return ds;
        }

        public void doSQL(string sSQL)
        {
            if (con.State == ConnectionState.Closed)
                con.Open();
            SqlCommand cmd = new SqlCommand(sSQL, con);
            cmd.Parameters.Clear();
            foreach (KeyValuePair<string, object> dic in paramList)
                cmd.Parameters.AddWithValue(dic.Key, dic.Value);
            if (dbTran != null)
                cmd.Transaction = dbTran;
            cmd.ExecuteNonQuery();
            clearParam();
            if (dbTran == null)
                con.Close();
        }

        public void addParam(string param, object value)
        {
            paramList.Add(param, value);
        }

        public void clearParam()
        {
            paramList.Clear();
        }
    }


}
