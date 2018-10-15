/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.sparkk8sdemo;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
/**
 *
 * @author ellenk
 */
public class SimpleSparkApp {
  
public static void main(String args[]) {
     SimpleSparkApp app = new SimpleSparkApp();
     SparkSession session = SparkSession
                .builder()
                .appName("WordCount")
                .getOrCreate();
     String readFileURI = session.sparkContext().getConf().get("spark.codeOne.demo.readFileURI");
     app.run(session, readFileURI);
      
    }
 
    public void run(SparkSession session, String readFileURI) {
      
        Dataset<Row> textRows = loadCloudText(session, readFileURI);
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
        HashingTF  hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("features");
        LDA lda = new LDA().setK(10).setMaxIterations(50);

         Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, stopWordsRemover, hashingTF});
    
        // Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(textRows);
    
        Dataset<Row> wordsData = 
                 tokenizer.transform(textRows)
                         .withColumn("word",explode(col("words")))
                         .groupBy(col("word"))
                         .count()
                         .orderBy(col("count").desc());
         wordsData.show();
       //     StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
     //   Dataset<Row> filtered = stopWordsRemover.transform(tokenized);

         System.out.println("wordsData distinct words " + wordsData.count() + " counted");

    }

    private Dataset<Row> loadCloudText(SparkSession session, String fromURI) {
        Dataset<Row> textRows=null;
        textRows = session.read().format("csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("delimiter", ",")
                    .load(fromURI);
      return textRows;
        
}
    private Dataset<Row> loadText(SparkSession session, String text) {
        List<Row> simple = Arrays.asList(RowFactory.create(text));
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
        JavaRDD<Row> rdd = jsc.parallelize(simple);
        StructType schema = new StructType();
        schema = schema.add("text", DataTypes.StringType);
        Dataset<Row> textDF = session.createDataFrame(rdd.rdd(), schema);
        return textDF;
    }
    
    
    
    

 private String bushSotuText = "Thank you very much. Mr. Speaker, Vice President Cheney, members of Congress, distinguished guests, fellow citizens: As we gather tonight, our nation is at war, our economy is in recession, and the civilized world faces unprecedented dangers. Yet the state of our Union has never been stronger.\n" +
"\n" +
"We last met in an hour of shock and suffering. In four short months, our nation has comforted the victims, begun to rebuild New York and the Pentagon, rallied a great coalition, captured, arrested, and rid the world of thousands of terrorists, destroyed Afghanistan's terrorist training camps, saved a people from starvation, and freed a country from brutal oppression.\n" +
"\n" +
"The American flag flies again over our embassy in Kabul. Terrorists who once occupied Afghanistan now occupy cells at Guantanamo Bay. And terrorist leaders who urged followers to sacrifice their lives are running for their own.\n" +
"\n" +
"America and Afghanistan are now allies against terror. We'll be partners in rebuilding that country. And this evening we welcome the distinguished interim leader of a liberated Afghanistan: Chairman Hamid Karzai.\n" +
"\n" +
"The last time we met in this chamber, the mothers and daughters of Afghanistan were captives in their own homes, forbidden from working or going to school. Today women are free, and are part of Afghanistan's new government. And we welcome the new Minister of Women's Affairs, Doctor Sima Samar.\n" +
"\n" +
"Our progress is a tribute to the spirit of the Afghan people, to the resolve of our coalition, and to the might of the United States military. When I called our troops into action, I did so with complete confidence in their courage and skill. And tonight, thanks to them, we are winning the war on terror. The men and women of our Armed Forces have delivered a message now clear to every enemy of the United States: Even 7,000 miles away, across oceans and continents, on mountaintops and in caves — you will not escape the justice of this nation.\n" +
"\n" +
"For many Americans, these four months have brought sorrow, and pain that will never completely go away. Every day a retired firefighter returns to Ground Zero, to feel closer to his two sons who died there. At a memorial in New York, a little boy left his football with a note for his lost father: Dear Daddy, please take this to heaven. I don't want to play football until I can play with you again some day.\n" +
"\n" +
"Last month, at the grave of her husband, Michael, a CIA officer and Marine who died in Mazur-e-Sharif, Shannon Spann said these words of farewell: \"Semper Fi, my love.\" Shannon is with us tonight.\n" +
"\n" +
"Shannon, I assure you and all who have lost a loved one that our cause is just, and our country will never forget the debt we owe Michael and all who gave their lives for freedom.\n" +
"\n" +
"Our cause is just, and it continues. Our discoveries in Afghanistan confirmed our worst fears, and showed us the true scope of the task ahead. We have seen the depth of our enemies' hatred in videos, where they laugh about the loss of innocent life. And the depth of their hatred is equaled by the madness of the destruction they design. We have found diagrams of American nuclear power plants and public water facilities, detailed instructions for making chemical weapons, surveillance maps of American cities, and thorough descriptions of landmarks in America and throughout the world.\n" +
"\n" +
"What we have found in Afghanistan confirms that, far from ending there, our war against terror is only beginning. Most of the 19 men who hijacked planes on September the 11th were trained in Afghanistan's camps, and so were tens of thousands of others. Thousands of dangerous killers, schooled in the methods of murder, often supported by outlaw regimes, are now spread throughout the world like ticking time bombs, set to go off without warning.\n" +
"\n" +
"Thanks to the work of our law enforcement officials and coalition partners, hundreds of terrorists have been arrested. Yet, tens of thousands of trained terrorists are still at large. These enemies view the entire world as a battlefield, and we must pursue them wherever they are. So long as training camps operate, so long as nations harbor terrorists, freedom is at risk. And America and our allies must not, and will not, allow it.\n" +
"\n" +
"Our nation will continue to be steadfast and patient and persistent in the pursuit of two great objectives. First, we will shut down terrorist camps, disrupt terrorist plans, and bring terrorists to justice. And, second, we must prevent the terrorists and regimes who seek chemical, biological or nuclear weapons from threatening the United States and the world.\n" +
"\n" +
"Our military has put the terror training camps of Afghanistan out of business, yet camps still exist in at least a dozen countries. A terrorist underworld — including groups like Hamas, Hezbollah, Islamic Jihad, Jaish-i-Mohammed — operates in remote jungles and deserts, and hides in the centers of large cities.\n" +
"\n" +
"While the most visible military action is in Afghanistan, America is acting elsewhere. We now have troops in the Philippines, helping to train that country's armed forces to go after terrorist cells that have executed an American, and still hold hostages. Our soldiers, working with the Bosnian government, seized terrorists who were plotting to bomb our embassy. Our Navy is patrolling the coast of Africa to block the shipment of weapons and the establishment of terrorist camps in Somalia.\n" +
"\n" +
"My hope is that all nations will heed our call, and eliminate the terrorist parasites who threaten their countries and our own. Many nations are acting forcefully. Pakistan is now cracking down on terror, and I admire the strong leadership of President Musharraf.\n" +
"\n" +
"But some governments will be timid in the face of terror. And make no mistake about it: If they do not act, America will.\n" +
"\n" +
"Our second goal is to prevent regimes that sponsor terror from threatening America or our friends and allies with weapons of mass destruction. Some of these regimes have been pretty quiet since September the 11th. But we know their true nature. North Korea is a regime arming with missiles and weapons of mass destruction, while starving its citizens.\n" +
"\n" +
"Iran aggressively pursues these weapons and exports terror, while an unelected few repress the Iranian people's hope for freedom.\n" +
"\n" +
"Iraq continues to flaunt its hostility toward America and to support terror. The Iraqi regime has plotted to develop anthrax, and nerve gas, and nuclear weapons for over a decade. This is a regime that has already used poison gas to murder thousands of its own citizens — leaving the bodies of mothers huddled over their dead children. This is a regime that agreed to international inspections — then kicked out the inspectors. This is a regime that has something to hide from the civilized world.\n" +
"\n" +
"States like these, and their terrorist allies, constitute an axis of evil, arming to threaten the peace of the world. By seeking weapons of mass destruction, these regimes pose a grave and growing danger. They could provide these arms to terrorists, giving them the means to match their hatred. They could attack our allies or attempt to blackmail the United States. In any of these cases, the price of indifference would be catastrophic.\n" +
"\n" +
"We will work closely with our coalition to deny terrorists and their state sponsors the materials, technology, and expertise to make and deliver weapons of mass destruction. We will develop and deploy effective missile defenses to protect America and our allies from sudden attack. And all nations should know: America will do what is necessary to ensure our nation's security.\n" +
"\n" +
"We'll be deliberate, yet time is not on our side. I will not wait on events, while dangers gather. I will not stand by, as peril draws closer and closer. The United States of America will not permit the world's most dangerous regimes to threaten us with the world's most destructive weapons.\n" +
"\n" +
"Our war on terror is well begun, but it is only begun. This campaign may not be finished on our watch — yet it must be and it will be waged on our watch.\n" +
"\n" +
"We can't stop short. If we stop now — leaving terror camps intact and terror states unchecked — our sense of security would be false and temporary. History has called America and our allies to action, and it is both our responsibility and our privilege to fight freedom's fight.\n" +
"\n" +
"Our first priority must always be the security of our nation, and that will be reflected in the budget I send to Congress. My budget supports three great goals for America: We will win this war; we'll protect our homeland; and we will revive our economy.\n" +
"\n" +
"September the 11th brought out the best in America, and the best in this Congress. And I join the American people in applauding your unity and resolve. Now Americans deserve to have this same spirit directed toward addressing problems here at home. I'm a proud member of my party — yet as we act to win the war, protect our people, and create jobs in America, we must act, first and foremost, not as Republicans, not as Democrats, but as Americans.\n" +
"\n" +
"It costs a lot to fight this war. We have spent more than a billion dollars a month — over $30 million a day — and we must be prepared for future operations. Afghanistan proved that expensive precision weapons defeat the enemy and spare innocent lives, and we need more of them. We need to replace aging aircraft and make our military more agile, to put our troops anywhere in the world quickly and safely. Our men and women in uniform deserve the best weapons, the best equipment, the best training — and they also deserve another pay raise.\n" +
"\n" +
"My budget includes the largest increase in defense spending in two decades — because while the price of freedom and security is high, it is never too high. Whatever it costs to defend our country, we will pay.\n" +
"\n" +
"The next priority of my budget is to do everything possible to protect our citizens and strengthen our nation against the ongoing threat of another attack. Time and distance from the events of September the 11th will not make us safer unless we act on its lessons. America is no longer protected by vast oceans. We are protected from attack only by vigorous action abroad, and increased vigilance at home.\n" +
"\n" +
"My budget nearly doubles funding for a sustained strategy of homeland security, focused on four key areas: bioterrorism, emergency response, airport and border security, and improved intelligence. We will develop vaccines to fight anthrax and other deadly diseases. We'll increase funding to help states and communities train and equip our heroic police and firefighters. We will improve intelligence collection and sharing, expand patrols at our borders, strengthen the security of air travel, and use technology to track the arrivals and departures of visitors to the United States.\n" +
"\n" +
"Homeland security will make America not only stronger, but, in many ways, better. Knowledge gained from bioterrorism research will improve public health. Stronger police and fire departments will mean safer neighborhoods. Stricter border enforcement will help combat illegal drugs. And as government works to better secure our homeland, America will continue to depend on the eyes and ears of alert citizens.\n" +
"\n" +
"A few days before Christmas, an airline flight attendant spotted a passenger lighting a match. The crew and passengers quickly subdued the man, who had been trained by al Qaeda and was armed with explosives. The people on that plane were alert and, as a result, likely saved nearly 200 lives. And tonight we welcome and thank flight attendants Hermis Moutardier and Christina Jones.\n" +
"\n" +
"Once we have funded our national security and our homeland security, the final great priority of my budget is economic security for the American people. To achieve these great national objectives — to win the war, protect the homeland, and revitalize our economy — our budget will run a deficit that will be small and short-term, so long as Congress restrains spending and acts in a fiscally responsible manner. We have clear priorities and we must act at home with the same purpose and resolve we have shown overseas: We'll prevail in the war, and we will defeat this recession.\n" +
"\n" +
"Americans who have lost their jobs need our help and I support extending unemployment benefits and direct assistance for health care coverage. Yet, American workers want more than unemployment checks — they want a steady paycheck. When America works, America prospers, so my economic security plan can be summed up in one word: jobs.\n" +
"\n" +
"Good jobs begin with good schools, and here we've made a fine start. Republicans and Democrats worked together to achieve historic education reform so that no child is left behind. I was proud to work with members of both parties: Chairman John Boehner and Congressman George Miller. Senator Judd Gregg. And I was so proud of our work, I even had nice things to say about my friend, Ted Kennedy. (Laughter and applause.) I know the folks at the Crawford coffee shop couldn't believe I'd say such a thing — (laughter) — but our work on this bill shows what is possible if we set aside posturing and focus on results.\n" +
"\n" +
"There is more to do. We need to prepare our children to read and succeed in school with improved Head Start and early childhood development programs. We must upgrade our teacher colleges and teacher training and launch a major recruiting drive with a great goal for America: a quality teacher in every classroom.\n" +
"\n" +
"Good jobs also depend on reliable and affordable energy. This Congress must act to encourage conservation, promote technology, build infrastructure, and it must act to increase energy production at home so America is less dependent on foreign oil.\n" +
"\n" +
"Good jobs depend on expanded trade. Selling into new markets creates new jobs, so I ask Congress to finally approve trade promotion authority. On these two key issues, trade and energy, the House of Representatives has acted to create jobs, and I urge the Senate to pass this legislation.\n" +
"\n" +
"Good jobs depend on sound tax policy. Last year, some in this hall thought my tax relief plan was too small; some thought it was too big. But when the checks arrived in the mail, most Americans thought tax relief was just about right. Congress listened to the people and responded by reducing tax rates, doubling the child credit, and ending the death tax. For the sake of long-term growth and to help Americans plan for the future, let's make these tax cuts permanent.\n" +
"\n" +
"The way out of this recession, the way to create jobs, is to grow the economy by encouraging investment in factories and equipment, and by speeding up tax relief so people have more money to spend. For the sake of American workers, let's pass a stimulus package.\n" +
"\n" +
"Good jobs must be the aim of welfare reform. As we reauthorize these important reforms, we must always remember the goal is to reduce dependency on government and offer every American the dignity of a job.\n" +
"\n" +
"Americans know economic security can vanish in an instant without health security. I ask Congress to join me this year to enact a patients' bill of rights — (applause) — to give uninsured workers credits to help buy health coverage — (applause) — to approve an historic increase in the spending for veterans' health — (applause) — and to give seniors a sound and modern Medicare system that includes coverage for prescription drugs.\n" +
"\n" +
"A good job should lead to security in retirement. I ask Congress to enact new safeguards for 401K and pension plans. Employees who have worked hard and saved all their lives should not have to risk losing everything if their company fails. Through stricter accounting standards and tougher disclosure requirements, corporate America must be made more accountable to employees and shareholders and held to the highest standards of conduct.\n" +
"\n" +
"Retirement security also depends upon keeping the commitments of Social Security, and we will. We must make Social Security financially stable and allow personal retirement accounts for younger workers who choose them.\n" +
"\n" +
"Members, you and I will work together in the months ahead on other issues: productive farm policy — (applause) — a cleaner environment — (applause) — broader home ownership, especially among minorities — (applause) — and ways to encourage the good work of charities and faith-based groups. I ask you to join me on these important domestic issues in the same spirit of cooperation we've applied to our war against terrorism.\n" +
"\n" +
"During these last few months, I've been humbled and privileged to see the true character of this country in a time of testing. Our enemies believed America was weak and materialistic, that we would splinter in fear and selfishness. They were as wrong as they are evil.\n" +
"\n" +
"The American people have responded magnificently, with courage and compassion, strength and resolve. As I have met the heroes, hugged the families, and looked into the tired faces of rescuers, I have stood in awe of the American people.\n" +
"\n" +
"And I hope you will join me — I hope you will join me in expressing thanks to one American for the strength and calm and comfort she brings to our nation in crisis, our First Lady, Laura Bush.\n" +
"\n" +
"None of us would ever wish the evil that was done on September the 11th. Yet after America was attacked, it was as if our entire country looked into a mirror and saw our better selves. We were reminded that we are citizens, with obligations to each other, to our country, and to history. We began to think less of the goods we can accumulate, and more about the good we can do.\n" +
"\n" +
"For too long our culture has said, \"If it feels good, do it.\" Now America is embracing a new ethic and a new creed: \"Let's roll.\" In the sacrifice of soldiers, the fierce brotherhood of firefighters, and the bravery and generosity of ordinary citizens, we have glimpsed what a new culture of responsibility could look like. We want to be a nation that serves goals larger than self. We've been offered a unique opportunity, and we must not let this moment pass.\n" +
"\n" +
"My call tonight is for every American to commit at least two years — 4,000 hours over the rest of your lifetime — to the service of your neighbors and your nation. Many are already serving, and I thank you. If you aren't sure how to help, I've got a good place to start. To sustain and extend the best that has emerged in America, I invite you to join the new USA Freedom Corps. The Freedom Corps will focus on three areas of need: responding in case of crisis at home; rebuilding our communities; and extending American compassion throughout the world.\n" +
"\n" +
"One purpose of the USA Freedom Corps will be homeland security. America needs retired doctors and nurses who can be mobilized in major emergencies; volunteers to help police and fire departments; transportation and utility workers well-trained in spotting danger.\n" +
"\n" +
"Our country also needs citizens working to rebuild our communities. We need mentors to love children, especially children whose parents are in prison. And we need more talented teachers in troubled schools. USA Freedom Corps will expand and improve the good efforts of AmeriCorps and Senior Corps to recruit more than 200,000 new volunteers.\n" +
"\n" +
"And America needs citizens to extend the compassion of our country to every part of the world. So we will renew the promise of the Peace Corps, double its volunteers over the next five years — (applause) — and ask it to join a new effort to encourage development and education and opportunity in the Islamic world.\n" +
"\n" +
"This time of adversity offers a unique moment of opportunity — a moment we must seize to change our culture. Through the gathering momentum of millions of acts of service and decency and kindness, I know we can overcome evil with greater good. And we have a great opportunity during this time of war to lead the world toward the values that will bring lasting peace.\n" +
"\n" +
"All fathers and mothers, in all societies, want their children to be educated, and live free from poverty and violence. No people on Earth yearn to be oppressed, or aspire to servitude, or eagerly await the midnight knock of the secret police.\n" +
"\n" +
"If anyone doubts this, let them look to Afghanistan, where the Islamic \"street\" greeted the fall of tyranny with song and celebration. Let the skeptics look to Islam's own rich history, with its centuries of learning, and tolerance and progress. America will lead by defending liberty and justice because they are right and true and unchanging for all people everywhere.\n" +
"\n" +
"No nation owns these aspirations, and no nation is exempt from them. We have no intention of imposing our culture. But America will always stand firm for the non-negotiable demands of human dignity: the rule of law; limits on the power of the state; respect for women; private property; free speech; equal justice; and religious tolerance.\n" +
"\n" +
"America will take the side of brave men and women who advocate these values around the world, including the Islamic world, because we have a greater objective than eliminating threats and containing resentment. We seek a just and peaceful world beyond the war on terror.\n" +
"\n" +
"In this moment of opportunity, a common danger is erasing old rivalries. America is working with Russia and China and India, in ways we have never before, to achieve peace and prosperity. In every region, free markets and free trade and free societies are proving their power to lift lives. Together with friends and allies from Europe to Asia, and Africa to Latin America, we will demonstrate that the forces of terror cannot stop the momentum of freedom.\n" +
"\n" +
"The last time I spoke here, I expressed the hope that life would return to normal. In some ways, it has. In others, it never will. Those of us who have lived through these challenging times have been changed by them. We've come to know truths that we will never question: evil is real, and it must be opposed. Beyond all differences of race or creed, we are one country, mourning together and facing danger together. Deep in the American character, there is honor, and it is stronger than cynicism. And many have discovered again that even in tragedy — especially in tragedy — God is near.\n" +
"\n" +
"In a single instant, we realized that this will be a decisive decade in the history of liberty, that we've been called to a unique role in human events. Rarely has the world faced a choice more clear or consequential.\n" +
"\n" +
"Our enemies send other people's children on missions of suicide and murder. They embrace tyranny and death as a cause and a creed. We stand for a different choice, made long ago, on the day of our founding. We affirm it again today. We choose freedom and the dignity of every life.\n" +
"\n" +
"Steadfast in our purpose, we now press on. We have known freedom's price. We have shown freedom's power. And in this great conflict, my fellow Americans, we will see freedom's victory.\n" +
"\n" +
"Thank you all. May God bless.";
}
