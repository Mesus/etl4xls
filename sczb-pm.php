<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>数据汇总</title>
</head>
<?php
header("Content-Type: text/html;charset=utf-8");

require('config.php');
session_start(); 

 $s = "";
 $farm = $_SESSION['farm'];
// echo "======SESSION:".$farm.";";
$get_farm = $_GET['farm'];
if($farm == ""){
	 $farm = $_GET['farm'];
	 $_SESSION['farm'] = $farm;

}else if($farm != $get_farm){
	// echo "======_GET:".$_GET['farm'];
	if($get_farm != ""){
	  $farm = $get_farm;
	 $_SESSION['farm'] = $farm;
	}

}
 $ff = "";
 switch($farm){  
    case 1:  
        $ff = '北京';  
        break; 
    case 2:
    	 $ff ='上海';
    	 break;
    case 3:  
        $ff = '广州';  
        break; 
    case 4:
    	 $ff ='成都';
    	 break;
    case 5:  
        $ff = '惠州';  
        break; 
    case 6:
    	 $ff ='扬州';
    	 break;
    case 7:  
        $ff = '云南';  
        break; 
    case 8:
    	 $ff ='山东';
    	 break;    	     	     	 
    default:  
        $ff = '';  
        break;  
  }
 if($ff !== ""){
 	$s = "and farm='".$ff."'";
 }
 // echo $s;
 
$sel_pl = $_SESSION['sel_pl'];
// echo "======".$sel_pl;
// echo "////////////".$_GET['sel_pl'];
if($sel_pl == ""){
	 $sel_pl = $_GET['sel_pl'];
	 $_SESSION['sel_pl'] = $sel_pl;

}else if($sel_pl != empty($_GET['sel_pl'])){
	 $sel_pl = $_GET['sel_pl'];
	 $_SESSION['sel_pl'] = $sel_pl;
}

 switch($sel_pl){  
	    case 1:  
	        $plname = '根茎类';
	        break; 
	    case 2:
	    	 $plname ='叶菜';
	    	 break;
	    case 3:  
	        $plname = '果菜';  
	        break;    	     	     	 
	    default:  
	        $plname = '';  
	        break;  
  	}

?>
<body>

  <table width="1308" height="233" border="1" align="center" cellspacing="0" style="font-size:19px">
  <?php
   $time = time();

 // $ntime = "20151115";
    $ntime = $_GET['rq'];
    if($ntime == ""){
  //   	$rst_time=pg_query($dbconn, "SELECT max(substring(insert_time,0,9)) FROM b2mcczb");
			
		// while($row=pg_fetch_row($rst_time)){
		// 	$ntime = $row[0];
		// }
		 $ntime = date("Y-m-d",$time);
    }
   // echo "==================".$ntime;
  $ntime = str_replace("-", "", $ntime);
 $strsj=($ntime==""?"":" and insert_time>='".$ntime."00' and insert_time<='".$ntime."24'");
 // $time = time();
 // $ntime = date("Ymd",$time);
 // $ntime = "20151115";
 // echo $pl_name;
 if($plname !== ""){
 	$pl_str = "and pl.pl_name='".$plname."' ";
 }
 
  $sql = "select sum(b.mcczl), sum(c.jpczkc), sum(c.jpccksl), sum(c.spczk), sum(c.spccksl),sum(c.mryjccsl)
					from pl,pm
					left join
					(
						select pmdm,sum(mcczl) as mcczl
						from
						(
							select row_number() over(partition by farm,pmdm order by insert_time desc) as rownum,pmdm,mcczl,farm,insert_time
							from 
							(
								select rtrim(pmdm) as pmdm,sum(mcczl) as mcczl,farm,insert_time
								from b2mcczb
								where 1=1 ".$s." ".$strsj." group by farm,insert_time,pmdm
							) as Test
						) as T
						where T.rownum=1
						group by pmdm
					) b
					on pm.id=b.pmdm
					left join
					(
						select pmdm,sum(jpczkc) as jpczkc,sum(jpccksl) as jpccksl,sum(spccksl) as spccksl,sum(mryjccsl) as mryjccsl,sum(spczk) as spczk
						from
						(
							select row_number() over(partition by farm,pmdm order by insert_time desc) as rownum,pmdm,jpczkc,jpccksl,spccksl,mryjccsl,spczk
							from b2yjsccs where 1=1 ".$s." ".$strsj."
						) as T
						where T.rownum=1
						group by pmdm
					) c
					on pm.id=c.pmdm
					where pl.id=pm.pl_id and (b.mcczl+c.jpczkc+c.jpccksl+c.spccksl+c.mryjccsl+c.spczk)>0 ".$pl_str."
					";
	// printf($sql);
$result=pg_query($dbconn, $sql);
			
while($row=pg_fetch_row($result))
			{?>
				    <tr>
        <td colspan="4" scope="col" align="center">合计：</td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[0])?></strong></td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[1])?></strong></td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[2])?></strong></td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[3])?></strong></td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[4])?></strong></td>
		<td align="right"><strong><?php echo sprintf("%.3f", $row[5])?></strong></td>
    </tr>
			<?php
		}

?>

   

    <tr>
      <th width="57" height="49" scope="col">序号</th>
      <th width="88" scope="col">品名代码</th>
      <th width="270" scope="col">品名</th>
      <th width="78" scope="col">品类</th>
      <th width="113" scope="col"><p>毛菜采摘量(Kg)</p>      </th>
      <th width="131" scope="col">精品菜库存量(Kg)</th>
      <th width="122" scope="col">精品菜出库量(Kg)</th>
      <th width="131" scope="col">商品菜库存量(Kg)</th>
      <th width="118" scope="col">商品菜出库量(Kg)</th>
      <th width="158" scope="col">明日预计出菜数量(Kg)</th>
    </tr>
    
    <?php
$Page_size=10;
    
        	
    	
	$sql="select ROW_NUMBER () OVER () AS xh,pm.id,pm.pm_name,pl.pl_name
					,(case when b.mcczl is null then 0 else b.mcczl end) as mcczl
					,(case when c.jpczkc is null then 0 else c.jpczkc end) as jpczkc
					,(case when c.jpccksl is null then 0 else c.jpccksl end) as jpccksl
					,(case when c.spczk is null then 0 else c.spczk end) as spczk
					,(case when c.spccksl is null then 0 else c.spccksl end) as spccksl
					,(case when c.mryjccsl is null then 0 else c.mryjccsl end) as mryjccsl
					from pl,pm
					left join
					(
						select pmdm,sum(mcczl) as mcczl
						from
						(
							select row_number() over(partition by farm,pmdm order by insert_time desc) as rownum,pmdm,mcczl,farm,insert_time
							from 
							(
								select rtrim(pmdm) as pmdm,sum(mcczl) as mcczl,farm,insert_time
								from b2mcczb
								where 1=1 ".$s." ".$strsj." group by farm,insert_time,pmdm
							) as Test
						) as T
						where T.rownum=1
						group by pmdm
					) b
					on pm.id=b.pmdm
					left join
					(
						select pmdm,sum(jpcjgsl) as jpcjgsl,sum(spcjgsl) as spcjgsl,sum(jpczkc) as jpczkc,sum(jpccksl) as jpccksl,sum(spccksl) as spccksl,sum(mryjccsl) as mryjccsl,sum(spczk) as spczk
						from
						(
							select row_number() over(partition by farm,pmdm order by insert_time desc) as rownum,pmdm,jpcjgsl,spcjgsl,jpczkc,jpccksl,spccksl,mryjccsl,spczk
							from b2yjsccs where 1=1 ".$s." ".$strsj."
						) as T
						where T.rownum=1
						group by pmdm
					) c
					on pm.id=c.pmdm
					where pl.id=pm.pl_id and (b.mcczl+c.jpczkc+c.jpccksl+c.spccksl+c.mryjccsl+c.spczk)>0 ".$pl_str."
					group by pm.id,pm.pm_name,pl.pl_name,b.mcczl,jpczkc,jpccksl,spczk,spccksl,mryjccsl
					order by pm.id";
	// echo $sql;
	$result=pg_query($dbconn, $sql);
	
			$count = pg_num_rows($result); 
$page_count = ceil($count/$Page_size); 
$init=1; 
$page_len=7; 
$max_p=$page_count; 
$pages=$page_count; 

//判断当前页码 
if(empty($_GET['page'])||$_GET['page']<0){ 
$page=1; 
}else { 
$page=$_GET['page']; 
} 
// echo $page;
$offset=$Page_size*($page-1); 
$sql = $sql." LIMIT $Page_size OFFSET $offset";
// echo $sql;
$result=pg_query($dbconn, $sql);
while($row=pg_fetch_row($result))
			{?>
				    <tr>
        <td><?php echo $row[0]?></td>
		<td><?php echo $row[1]?></td>
		<td><?php echo $row[2]?></td>
		<td><?php echo $row[3]?></td>
		<td align="right"><?php echo sprintf("%.3f", $row[4])?></td>
		<td align="right"><?php echo sprintf("%.3f",$row[5])?></td>
		<td align="right"><?php echo sprintf("%.3f",$row[6])?></td>
		<td align="right"><?php echo sprintf("%.3f",$row[7])?></td>
		<td align="right"><?php echo sprintf("%.3f",$row[8])?></td>
		<td align="right"><?php echo sprintf("%.3f",$row[9])?></td>
    </tr>
			<?php
		}
		$page_len = ($page_len%2)?$page_len:$pagelen+1;//页码个数 
$pageoffset = ($page_len-1)/2;//页码个数左右偏移量 

$key='<div class="page">'; 
$key.="<span>$page/$pages</span> "; //第几页,共几页 
if($page!=1){ 
$key.="<a href=\"sczb-pm.php?farm=".$_GET['farm']."&rq=".$_GET['rq']."&sel_pl=".$_GET['sel_pl']."&page=1\">第一页</a> "; //第一页 
$key.="<a href=\"sczb-pm.php?farm=".$_GET['farm']."&rq=".$_GET['rq']."&sel_pl=".$_GET['sel_pl']."&page=".($page-1)."\">上一页</a>"; //上一页 
}else { 
$key.="第一页 ";//第一页 
$key.="上一页"; //上一页 
} 
if($pages>$page_len){ 
//如果当前页小于等于左偏移 
if($page<=$pageoffset){ 
$init=1; 
$max_p = $page_len; 
}else{//如果当前页大于左偏移 
//如果当前页码右偏移超出最大分页数 
if($page+$pageoffset>=$pages+1){ 
$init = $pages-$page_len+1; 
}else{ 
//左右偏移都存在时的计算 
$init = $page-$pageoffset; 
$max_p = $page+$pageoffset; 
} 
} 
} 
for($i=$init;$i<=$max_p;$i++){ 
if($i==$page){ 
$key.=' <span>'.$i.'</span>'; 
} else { 
$key.=" <a href=\"sczb-pm.php?farm=".$_GET['farm']."&rq=".$_GET['rq']."&page=".$i."\">".$i."</a>"; 

} 
} 
if($page!=$pages){ 
$key.=" <a href=\"sczb-pm.php?farm=".$_GET['farm']."&rq=".$_GET['rq']."&sel_pl=".$_GET['sel_pl']."&page=".($page+1)."\">下一页</a> ";//下一页 

$key.="<a href=\"sczb-pm.php?farm=".$_GET['farm']."&rq=".$_GET['rq']."&sel_pl=".$_GET['sel_pl']."&page={$pages}\">最后一页</a>"; //最后一页 
}else { 
$key.="下一页 ";//下一页 
$key.="最后一页"; //最后一页 
} 
$key.='</div>'; 
		?>
		<tr> 
<td colspan="10" bgcolor="#E0EEE0" ><div align="center"><?php echo $key?></div></td> 
</tr> 
  </table>

</body>
</html>
